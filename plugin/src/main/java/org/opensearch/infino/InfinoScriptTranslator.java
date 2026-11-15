/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.infino.InfinoNative.Column;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.infino.InfinoQueryTranslator.literal;
import static org.opensearch.infino.InfinoQueryTranslator.quoteIdent;

/**
 * Turns the expression subset of a Painless script into one SQL expression
 * over the table's columns, so a scripted field, a script query, a scripted
 * aggregation source or a script sort runs in the engine as a column
 * expression instead of a per-document interpreter.
 *
 * <p>Read: literals; {@code doc['f'].value}, {@code .size()}, {@code .empty},
 * {@code .length}; {@code params.x} and {@code params['x']} as literals;
 * {@code _value} (the field of a value script) and {@code _source.f};
 * arithmetic, comparison, equality (with {@code null}), logical and ternary
 * operators; casts; {@code Math.*}, {@code Integer.parseInt} and the other
 * parse and {@code valueOf} calls; string methods (length, case, trim,
 * contains, startsWith, endsWith, indexOf, substring, replace, equals,
 * isEmpty, matches, concat, toString); date methods (toInstant, toEpochMilli,
 * getMillis, getTime, getHour, getMinute, getSecond, getDayOfMonth, getMonth,
 * getMonthValue, getYear, getDayOfWeek, getDayOfYear, and each as Painless's
 * getter shortcut: {@code .hour}, {@code .dayOfWeek}), {@code new Date(x)},
 * {@code Instant.ofEpochMilli}, {@code ChronoUnit.X.between} and
 * {@code System.currentTimeMillis}; statements: {@code def} / typed variable
 * declarations, {@code return}, {@code if} / {@code else} (as CASE), a
 * trailing expression, and the helper-function form OpenSearch Dashboards
 * emits for a filter on a scripted field ({@code boolean compare(Supplier s,
 * def v) {return s.get() == v;} compare(() -> { ... }, params.value)}).
 *
 * <p>Anything else — loops, assignments after a declaration, lists and maps,
 * methods outside that set, stored scripts, other languages — is refused
 * with the script named, never run elsewhere. A missing value is SQL NULL,
 * which the comparisons and CASE arms read as false; Painless would throw
 * unless the script guards with {@code size()}, which reads the same here.
 */
final class InfinoScriptTranslator {

    /** The type an expression evaluates to, as far as the emitter needs it. */
    enum SqlType {
        INT,
        FLOAT,
        STRING,
        BOOL,
        DATE,
        UNKNOWN;

        boolean isNumeric() {
            return this == INT || this == FLOAT;
        }
    }

    /** One SQL expression and its type. */
    record Sql(String expr, SqlType type) {
        static Sql of(Column c) {
            return new Sql(quoteIdent(c.name()), typeOf(c));
        }
    }

    /** The SQL type of a column. */
    static SqlType typeOf(Column c) {
        if (c.isString()) {
            return SqlType.STRING;
        }
        if (c.isBoolean()) {
            return SqlType.BOOL;
        }
        if (c.isTimestamp()) {
            return SqlType.DATE;
        }
        if (c.isNumeric()) {
            return "f64".equals(c.type()) ? SqlType.FLOAT : SqlType.INT;
        }
        return SqlType.UNKNOWN;
    }

    /** A script the table cannot run; the request fails with this reason. */
    static final class Unsupported extends IllegalArgumentException {
        Unsupported(String message) {
            super("[infino] script " + message);
        }
    }

    /** Milliseconds in a day, hour, minute and second, for ChronoUnit arithmetic. */
    private static final long DAY_MS = 86_400_000L;
    private static final long HOUR_MS = 3_600_000L;
    private static final long MINUTE_MS = 60_000L;
    private static final long SECOND_MS = 1_000L;
    /** What Painless puts before a member's name to call it as a getter ({@code .hour} is {@code getHour()}). */
    private static final String GETTER_PREFIX = "get";

    private final Map<String, Column> cols;
    private final Map<String, Object> params;
    private final Column valueColumn;
    private final long nowMillis;
    private final String source;

    private InfinoScriptTranslator(Map<String, Column> cols, Map<String, Object> params, Column valueColumn, long nowMillis, String source) {
        this.cols = cols;
        this.params = params == null ? Map.of() : params;
        this.valueColumn = valueColumn;
        this.nowMillis = nowMillis;
        this.source = source;
    }

    /**
     * The SQL expression of {@code script} over the columns; {@code valueColumn}
     * is what {@code _value} names (a value script of an aggregation), null
     * otherwise. Throws {@link Unsupported} naming what cannot be read.
     */
    static Sql translate(Script script, Map<String, Column> cols, Column valueColumn, long nowMillis) {
        if (script.getType() != ScriptType.INLINE) {
            throw new Unsupported("[" + script.getIdOrCode() + "] is a stored script; only inline scripts are served");
        }
        String lang = script.getLang();
        if (lang != null && !"painless".equals(lang) && !"expression".equals(lang)) {
            throw new Unsupported("language [" + lang + "] is not served; painless is");
        }
        String code = script.getIdOrCode();
        InfinoScriptTranslator t = new InfinoScriptTranslator(cols, script.getParams(), valueColumn, nowMillis, code);
        List<Stmt> statements = new Parser(code, t).program();
        Sql out = t.block(statements, new Scope(null), null);
        if (out == null) {
            throw t.refuse("returns nothing");
        }
        return out;
    }

    Unsupported refuse(String what) {
        return new Unsupported("[" + source + "] " + what);
    }

    // ---- syntax tree ----

    private sealed interface Expr permits Num, Str, Bool, Null, Ident, Access, Index, Call, Unary, Binary, Ternary, Cast, New, Lambda {}

    private record Num(String text, boolean integral) implements Expr {}

    private record Str(String value) implements Expr {}

    private record Bool(boolean value) implements Expr {}

    private record Null() implements Expr {}

    private record Ident(String name) implements Expr {}

    /** {@code target.name} (a property, no parentheses). */
    private record Access(Expr target, String name) implements Expr {}

    /** {@code target[index]}. */
    private record Index(Expr target, Expr index) implements Expr {}

    /** {@code target.name(args)}, or {@code name(args)} when target is null. */
    private record Call(Expr target, String name, List<Expr> args) implements Expr {}

    private record Unary(String op, Expr operand) implements Expr {}

    private record Binary(String op, Expr left, Expr right) implements Expr {}

    private record Ternary(Expr condition, Expr ifTrue, Expr ifFalse) implements Expr {}

    private record Cast(String type, Expr operand) implements Expr {}

    private record New(String className, List<Expr> args) implements Expr {}

    /** {@code () -> expr} or {@code () -> { statements }}. */
    private record Lambda(List<Stmt> body) implements Expr {}

    private sealed interface Stmt permits VarDecl, Return, If, ExprStmt, FuncDef {}

    private record VarDecl(String name, Expr value) implements Stmt {}

    private record Return(Expr value) implements Stmt {}

    private record If(Expr condition, List<Stmt> then, List<Stmt> otherwise) implements Stmt {}

    private record ExprStmt(Expr value) implements Stmt {}

    private record FuncDef(String name, List<String> params, List<Stmt> body) implements Stmt {}

    // ---- tokens ----

    private enum Kind {
        NUMBER,
        STRING,
        IDENT,
        OP,
        END
    }

    private record Token(Kind kind, String text, boolean integral) {}

    /** The operators, longest first so the longer match wins. */
    private static final String[] OPERATORS = {
        "===", "!==", "->", "&&", "||", "==", "!=", "<=", ">=", "(", ")", "[", "]", "{", "}", ",", ";", ".", "?", ":", "+", "-", "*", "/", "%", "!", "<", ">", "=" };

    private static List<Token> tokenize(String s, InfinoScriptTranslator t) {
        List<Token> out = new ArrayList<>();
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == '/' && i + 1 < n && s.charAt(i + 1) == '/') {
                while (i < n && s.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
                continue;
            }
            if (Character.isDigit(c) || (c == '.' && i + 1 < n && Character.isDigit(s.charAt(i + 1)))) {
                int start = i;
                boolean integral = true;
                while (i < n && Character.isDigit(s.charAt(i))) {
                    i++;
                }
                if (i < n && s.charAt(i) == '.' && i + 1 < n && Character.isDigit(s.charAt(i + 1))) {
                    integral = false;
                    i++;
                    while (i < n && Character.isDigit(s.charAt(i))) {
                        i++;
                    }
                }
                if (i < n && (s.charAt(i) == 'e' || s.charAt(i) == 'E')) {
                    int save = i;
                    i++;
                    if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) {
                        i++;
                    }
                    if (i < n && Character.isDigit(s.charAt(i))) {
                        integral = false;
                        while (i < n && Character.isDigit(s.charAt(i))) {
                            i++;
                        }
                    } else {
                        i = save;
                    }
                }
                String text = s.substring(start, i);
                if (i < n && "lLfFdD".indexOf(s.charAt(i)) >= 0) {
                    char suffix = Character.toLowerCase(s.charAt(i));
                    integral = integral && suffix == 'l';
                    i++;
                }
                out.add(new Token(Kind.NUMBER, text, integral));
                continue;
            }
            if (c == '\'' || c == '"') {
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < n && s.charAt(i) != c) {
                    char ch = s.charAt(i);
                    if (ch == '\\' && i + 1 < n) {
                        char e = s.charAt(i + 1);
                        sb.append(switch (e) {
                            case 'n' -> '\n';
                            case 't' -> '\t';
                            case 'r' -> '\r';
                            default -> e;
                        });
                        i += 2;
                        continue;
                    }
                    sb.append(ch);
                    i++;
                }
                if (i >= n) {
                    throw t.refuse("has an unterminated string");
                }
                i++;
                out.add(new Token(Kind.STRING, sb.toString(), false));
                continue;
            }
            if (Character.isLetter(c) || c == '_' || c == '$') {
                int start = i;
                while (i < n && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_' || s.charAt(i) == '$')) {
                    i++;
                }
                out.add(new Token(Kind.IDENT, s.substring(start, i), false));
                continue;
            }
            String op = null;
            for (String candidate : OPERATORS) {
                if (s.startsWith(candidate, i)) {
                    op = candidate;
                    break;
                }
            }
            if (op == null) {
                throw t.refuse("has a character the reader does not know: [" + c + "]");
            }
            out.add(new Token(Kind.OP, op, false));
            i += op.length();
        }
        out.add(new Token(Kind.END, "", false));
        return out;
    }

    // ---- parser ----

    /** Types a declaration or cast may name. */
    private static final List<String> TYPE_NAMES = List.of(
        "def",
        "int",
        "long",
        "short",
        "byte",
        "double",
        "float",
        "boolean",
        "String",
        "Object",
        "ZonedDateTime",
        "Instant",
        "Date",
        "Supplier"
    );

    private static final class Parser {
        private final List<Token> tokens;
        private final InfinoScriptTranslator t;
        private int pos;

        Parser(String code, InfinoScriptTranslator t) {
            this.tokens = tokenize(code, t);
            this.t = t;
        }

        private Token peek() {
            return tokens.get(pos);
        }

        private Token peek(int ahead) {
            return tokens.get(Math.min(pos + ahead, tokens.size() - 1));
        }

        private boolean at(String op) {
            Token tk = peek();
            return tk.kind() == Kind.OP && tk.text().equals(op);
        }

        private boolean atIdent(String name) {
            Token tk = peek();
            return tk.kind() == Kind.IDENT && tk.text().equals(name);
        }

        private boolean accept(String op) {
            if (at(op)) {
                pos++;
                return true;
            }
            return false;
        }

        private void expect(String op) {
            if (!accept(op)) {
                throw t.refuse("expects [" + op + "] but reads [" + peek().text() + "]");
            }
        }

        private String ident() {
            Token tk = peek();
            if (tk.kind() != Kind.IDENT) {
                throw t.refuse("expects a name but reads [" + tk.text() + "]");
            }
            pos++;
            return tk.text();
        }

        List<Stmt> program() {
            List<Stmt> out = statements();
            if (peek().kind() != Kind.END) {
                throw t.refuse("has text the reader does not know after the last statement: [" + peek().text() + "]");
            }
            return out;
        }

        /** Statements up to the end or a closing brace; a semicolon or a closing brace ends a statement. */
        private List<Stmt> statements() {
            List<Stmt> out = new ArrayList<>();
            while (peek().kind() != Kind.END && !at("}")) {
                if (accept(";")) {
                    continue;
                }
                out.add(statement());
                accept(";");
            }
            return out;
        }

        private Stmt statement() {
            Token tk = peek();
            if (tk.kind() == Kind.IDENT) {
                if (tk.text().equals("return")) {
                    pos++;
                    if (at(";") || at("}") || peek().kind() == Kind.END) {
                        throw t.refuse("returns nothing");
                    }
                    return new Return(expression());
                }
                if (tk.text().equals("if")) {
                    pos++;
                    expect("(");
                    Expr cond = expression();
                    expect(")");
                    List<Stmt> then = block();
                    List<Stmt> otherwise = null;
                    if (atIdent("else")) {
                        pos++;
                        otherwise = atIdent("if") ? List.of(statement()) : block();
                    }
                    return new If(cond, then, otherwise);
                }
                // `type name(type p, ...) { ... }`: a helper function.
                if (TYPE_NAMES.contains(tk.text()) && peek(1).kind() == Kind.IDENT && peek(2).kind() == Kind.OP && peek(2).text().equals("(")) {
                    pos++;
                    String name = ident();
                    expect("(");
                    List<String> params = new ArrayList<>();
                    while (!at(")")) {
                        if (TYPE_NAMES.contains(peek().text()) && peek(1).kind() == Kind.IDENT) {
                            pos++;
                        }
                        params.add(ident());
                        if (!accept(",")) {
                            break;
                        }
                    }
                    expect(")");
                    expect("{");
                    List<Stmt> body = statements();
                    expect("}");
                    return new FuncDef(name, params, body);
                }
                // `type name = expr`: a declaration.
                if (TYPE_NAMES.contains(tk.text()) && peek(1).kind() == Kind.IDENT && peek(2).kind() == Kind.OP && peek(2).text().equals("=")) {
                    pos++;
                    String name = ident();
                    expect("=");
                    return new VarDecl(name, expression());
                }
                if (tk.text().equals("for") || tk.text().equals("while") || tk.text().equals("do") || tk.text().equals("try") || tk.text().equals("throw")) {
                    throw t.refuse("uses [" + tk.text() + "], which is not read; only expressions, declarations, if/else and return are");
                }
            }
            Expr e = expression();
            if (at("=")) {
                throw t.refuse("assigns to a value after its declaration, which is not read");
            }
            return new ExprStmt(e);
        }

        private List<Stmt> block() {
            if (accept("{")) {
                List<Stmt> out = statements();
                expect("}");
                return out;
            }
            Stmt one = statement();
            accept(";");
            return List.of(one);
        }

        Expr expression() {
            Expr cond = or();
            if (accept("?")) {
                Expr a = expression();
                expect(":");
                Expr b = expression();
                return new Ternary(cond, a, b);
            }
            return cond;
        }

        private Expr or() {
            Expr left = and();
            while (accept("||")) {
                left = new Binary("||", left, and());
            }
            return left;
        }

        private Expr and() {
            Expr left = equality();
            while (accept("&&")) {
                left = new Binary("&&", left, equality());
            }
            return left;
        }

        private Expr equality() {
            Expr left = comparison();
            while (true) {
                if (accept("==") || accept("===")) {
                    left = new Binary("==", left, comparison());
                } else if (accept("!=") || accept("!==")) {
                    left = new Binary("!=", left, comparison());
                } else {
                    return left;
                }
            }
        }

        private Expr comparison() {
            Expr left = additive();
            while (true) {
                String op = null;
                for (String candidate : new String[] { "<=", ">=", "<", ">" }) {
                    if (at(candidate)) {
                        op = candidate;
                        break;
                    }
                }
                if (op == null) {
                    if (atIdent("instanceof")) {
                        throw t.refuse("uses instanceof, which is not read");
                    }
                    return left;
                }
                pos++;
                left = new Binary(op, left, additive());
            }
        }

        private Expr additive() {
            Expr left = multiplicative();
            while (true) {
                if (accept("+")) {
                    left = new Binary("+", left, multiplicative());
                } else if (accept("-")) {
                    left = new Binary("-", left, multiplicative());
                } else {
                    return left;
                }
            }
        }

        private Expr multiplicative() {
            Expr left = unary();
            while (true) {
                if (accept("*")) {
                    left = new Binary("*", left, unary());
                } else if (accept("/")) {
                    left = new Binary("/", left, unary());
                } else if (accept("%")) {
                    left = new Binary("%", left, unary());
                } else {
                    return left;
                }
            }
        }

        private Expr unary() {
            if (accept("!")) {
                return new Unary("!", unary());
            }
            if (accept("-")) {
                return new Unary("-", unary());
            }
            if (accept("+")) {
                return unary();
            }
            // A cast: `(type) expr`.
            if (at("(") && peek(1).kind() == Kind.IDENT && TYPE_NAMES.contains(peek(1).text()) && peek(2).kind() == Kind.OP && peek(2).text().equals(")")) {
                pos++;
                String type = ident();
                expect(")");
                return new Cast(type, unary());
            }
            return postfix(primary());
        }

        private Expr postfix(Expr e) {
            while (true) {
                if (accept(".")) {
                    String name = ident();
                    if (accept("(")) {
                        e = new Call(e, name, arguments());
                    } else {
                        e = new Access(e, name);
                    }
                } else if (accept("[")) {
                    Expr index = expression();
                    expect("]");
                    e = new Index(e, index);
                } else {
                    return e;
                }
            }
        }

        private List<Expr> arguments() {
            List<Expr> out = new ArrayList<>();
            if (accept(")")) {
                return out;
            }
            do {
                out.add(expression());
            } while (accept(","));
            expect(")");
            return out;
        }

        private Expr primary() {
            Token tk = peek();
            switch (tk.kind()) {
                case NUMBER -> {
                    pos++;
                    return new Num(tk.text(), tk.integral());
                }
                case STRING -> {
                    pos++;
                    return new Str(tk.text());
                }
                case IDENT -> {
                    pos++;
                    switch (tk.text()) {
                        case "true" -> {
                            return new Bool(true);
                        }
                        case "false" -> {
                            return new Bool(false);
                        }
                        case "null" -> {
                            return new Null();
                        }
                        case "new" -> {
                            String className = ident();
                            expect("(");
                            return new New(className, arguments());
                        }
                        default -> {
                            if (accept("(")) {
                                return new Call(null, tk.text(), arguments());
                            }
                            return new Ident(tk.text());
                        }
                    }
                }
                case OP -> {
                    if (at("(")) {
                        // `() -> ...`: a lambda; else a parenthesized expression.
                        if (peek(1).kind() == Kind.OP && peek(1).text().equals(")") && peek(2).kind() == Kind.OP && peek(2).text().equals("->")) {
                            pos += 3;
                            if (accept("{")) {
                                List<Stmt> body = statements();
                                expect("}");
                                return new Lambda(body);
                            }
                            return new Lambda(List.of(new Return(expression())));
                        }
                        pos++;
                        Expr e = expression();
                        expect(")");
                        return e;
                    }
                    throw t.refuse("reads [" + tk.text() + "] where a value should be");
                }
                default -> throw t.refuse("ends where a value should be");
            }
        }
    }

    // ---- emission ----

    /** Names bound in a block: variables to their SQL, helper functions to their definitions. */
    private static final class Scope {
        final Scope parent;
        final Map<String, Sql> vars = new HashMap<>();
        final Map<String, FuncDef> funcs = new HashMap<>();

        Scope(Scope parent) {
            this.parent = parent;
        }

        Sql var(String name) {
            for (Scope s = this; s != null; s = s.parent) {
                Sql v = s.vars.get(name);
                if (v != null) {
                    return v;
                }
            }
            return null;
        }

        FuncDef func(String name) {
            for (Scope s = this; s != null; s = s.parent) {
                FuncDef f = s.funcs.get(name);
                if (f != null) {
                    return f;
                }
            }
            return null;
        }
    }

    /**
     * The value a statement list evaluates to: declarations bind, a return
     * or a trailing expression is the value, and an {@code if} becomes a
     * CASE between its branches, a branch that falls through (or a missing
     * else) continuing with the statements after the {@code if}. A list that
     * runs out of statements yields {@code fallThrough}: what the enclosing
     * list continues with, null at the top.
     */
    private Sql block(List<Stmt> statements, Scope scope, Sql fallThrough) {
        for (int i = 0; i < statements.size(); i++) {
            Stmt s = statements.get(i);
            if (s instanceof VarDecl d) {
                scope.vars.put(d.name(), emit(d.value(), scope));
            } else if (s instanceof FuncDef f) {
                scope.funcs.put(f.name(), f);
            } else if (s instanceof Return r) {
                return emit(r.value(), scope);
            } else if (s instanceof ExprStmt e) {
                if (i == statements.size() - 1) {
                    return emit(e.value(), scope);
                }
                throw refuse("has an expression statement whose value is dropped: that is not read");
            } else if (s instanceof If cond) {
                Sql c = bool(emit(cond.condition(), scope));
                Sql rest = block(statements.subList(i + 1, statements.size()), new Scope(scope), fallThrough);
                Sql then = block(cond.then(), new Scope(scope), rest);
                Sql otherwise = cond.otherwise() != null ? block(cond.otherwise(), new Scope(scope), rest) : rest;
                if (then == null || otherwise == null) {
                    throw refuse("has an if after which a path yields no value");
                }
                return caseWhen(c, then, otherwise);
            }
        }
        return fallThrough;
    }

    private static Sql caseWhen(Sql condition, Sql a, Sql b) {
        SqlType type = a.type() == SqlType.UNKNOWN ? b.type() : a.type();
        if (a.type().isNumeric() && b.type().isNumeric() && a.type() != b.type()) {
            type = SqlType.FLOAT;
        }
        return new Sql("CASE WHEN " + condition.expr() + " THEN " + a.expr() + " ELSE " + b.expr() + " END", type);
    }

    private Sql emit(Expr e, Scope scope) {
        if (e instanceof Num n) {
            return new Sql(n.integral() ? n.text() : Double.toString(Double.parseDouble(n.text())), n.integral() ? SqlType.INT : SqlType.FLOAT);
        }
        if (e instanceof Str s) {
            return new Sql(literal(s.value()), SqlType.STRING);
        }
        if (e instanceof Bool b) {
            return new Sql(b.value() ? "TRUE" : "FALSE", SqlType.BOOL);
        }
        if (e instanceof Null) {
            return new Sql("NULL", SqlType.UNKNOWN);
        }
        if (e instanceof Ident id) {
            return identifier(id.name(), scope);
        }
        if (e instanceof Index ix) {
            return index(ix, scope);
        }
        if (e instanceof Access a) {
            return access(a, scope);
        }
        if (e instanceof Call c) {
            return call(c, scope);
        }
        if (e instanceof Unary u) {
            Sql v = emit(u.operand(), scope);
            if (u.op().equals("!")) {
                return new Sql("NOT (" + bool(v).expr() + ")", SqlType.BOOL);
            }
            return new Sql("(-" + v.expr() + ")", v.type().isNumeric() ? v.type() : SqlType.FLOAT);
        }
        if (e instanceof Binary b) {
            return binary(b, scope);
        }
        if (e instanceof Ternary t) {
            return caseWhen(bool(emit(t.condition(), scope)), emit(t.ifTrue(), scope), emit(t.ifFalse(), scope));
        }
        if (e instanceof Cast c) {
            return cast(c.type(), emit(c.operand(), scope));
        }
        if (e instanceof New n) {
            return construct(n, scope);
        }
        if (e instanceof Lambda l) {
            Sql v = block(l.body(), new Scope(scope), null);
            if (v == null) {
                throw refuse("has a lambda that yields nothing");
            }
            return v;
        }
        throw refuse("has a form the reader does not know");
    }

    private Sql identifier(String name, Scope scope) {
        Sql v = scope.var(name);
        if (v != null) {
            return v;
        }
        if (name.equals("_value")) {
            if (valueColumn == null) {
                throw refuse("names _value outside a value script");
            }
            return Sql.of(valueColumn);
        }
        if (name.equals("doc") || name.equals("params") || name.equals("_source")) {
            throw refuse("uses [" + name + "] without a field");
        }
        throw refuse("names [" + name + "], which is not declared");
    }

    /** {@code doc['f']}, {@code params['x']} or a declared map. */
    private Sql index(Index ix, Scope scope) {
        String key = constantString(ix.index(), scope);
        if (ix.target() instanceof Ident id && scope.var(id.name()) == null) {
            switch (id.name()) {
                case "doc" -> {
                    return docField(key, null);
                }
                case "params" -> {
                    return param(key);
                }
                case "_source" -> {
                    return columnValue(key);
                }
                default -> {
                }
            }
        }
        throw refuse("indexes [" + describe(ix.target()) + "], which is not read");
    }

    private String constantString(Expr e, Scope scope) {
        if (e instanceof Str s) {
            return s.value();
        }
        Sql v = emit(e, scope);
        throw refuse("uses [" + v.expr() + "] as a key where a string literal is read");
    }

    /** {@code doc['f']} itself, or one of its properties when {@code property} is given. */
    private Sql docField(String field, String property) {
        Column c = column(field);
        if (property == null || property.equals("value") || property.equals("getValue")) {
            return c == null ? new Sql("NULL", SqlType.UNKNOWN) : Sql.of(c);
        }
        String col = c == null ? "NULL" : quoteIdent(c.name());
        return switch (property) {
            case "size", "length" -> new Sql("CASE WHEN " + col + " IS NULL THEN 0 ELSE 1 END", SqlType.INT);
            case "empty", "isEmpty" -> new Sql("(" + col + " IS NULL)", SqlType.BOOL);
            default -> throw refuse("reads doc['" + field + "']." + property + ", which is not served; .value, .size() and .empty are");
        };
    }

    private Column column(String field) {
        Column c = cols.get(field);
        if (c == null && field.endsWith(".keyword")) {
            Column base = cols.get(field.substring(0, field.length() - ".keyword".length()));
            if (base != null && base.isString()) {
                return base;
            }
        }
        return c;
    }

    private Sql columnValue(String field) {
        Column c = column(field);
        return c == null ? new Sql("NULL", SqlType.UNKNOWN) : Sql.of(c);
    }

    /** A parameter as a literal of its JSON type; a missing one is NULL. */
    private Sql param(String name) {
        Object v = params.get(name);
        if (v == null) {
            return new Sql("NULL", SqlType.UNKNOWN);
        }
        if (v instanceof Boolean b) {
            return new Sql(b ? "TRUE" : "FALSE", SqlType.BOOL);
        }
        if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) {
            return new Sql(Long.toString(((Number) v).longValue()), SqlType.INT);
        }
        if (v instanceof Number n) {
            return new Sql(Double.toString(n.doubleValue()), SqlType.FLOAT);
        }
        if (v instanceof String s) {
            return new Sql(literal(s), SqlType.STRING);
        }
        throw refuse("has a parameter [" + name + "] that is not a number, string or boolean");
    }

    private Sql access(Access a, Scope scope) {
        // doc['f'].value / .empty / .length, params.x, _source.f
        if (a.target() instanceof Index ix && ix.target() instanceof Ident id && id.name().equals("doc") && scope.var("doc") == null) {
            return docField(constantString(ix.index(), scope), a.name());
        }
        if (a.target() instanceof Ident id && scope.var(id.name()) == null) {
            switch (id.name()) {
                case "params" -> {
                    return param(a.name());
                }
                case "_source" -> {
                    return columnValue(a.name());
                }
                case "Math" -> {
                    return switch (a.name()) {
                        case "PI" -> new Sql("pi()", SqlType.FLOAT);
                        case "E" -> new Sql("exp(1.0)", SqlType.FLOAT);
                        default -> throw refuse("reads Math." + a.name() + ", which is not served");
                    };
                }
                default -> {
                }
            }
        }
        if (a.target() instanceof Access inner && inner.target() instanceof Ident id && id.name().equals("params") && scope.var("params") == null && inner.name().equals("_source")) {
            return columnValue(a.name());
        }
        Sql target = emit(a.target(), scope);
        return switch (a.name()) {
            case "millis" -> millis(target);
            case "length" -> target.type() == SqlType.STRING ? new Sql("length(" + target.expr() + ")", SqlType.INT) : noSuch(target, a.name());
            case "empty" -> new Sql("(" + target.expr() + " IS NULL)", SqlType.BOOL);
            // Painless reads any other member as its getter: a date's .hour is getHour().
            default -> method(target, GETTER_PREFIX + Character.toUpperCase(a.name().charAt(0)) + a.name().substring(1), List.of(), scope);
        };
    }

    private Sql noSuch(Sql target, String member) {
        throw refuse("reads ." + member + " of a " + target.type().name().toLowerCase(Locale.ROOT) + " value [" + target.expr() + "], which is not served");
    }

    private String describe(Expr e) {
        if (e instanceof Ident id) {
            return id.name();
        }
        return e.getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    private Sql call(Call c, Scope scope) {
        if (c.target() == null) {
            return function(c.name(), c.args(), scope);
        }
        // doc['f'].size() / .isEmpty() / .getValue()
        if (c.target() instanceof Index ix && ix.target() instanceof Ident id && id.name().equals("doc") && scope.var("doc") == null) {
            return docField(constantString(ix.index(), scope), c.name());
        }
        if (c.target() instanceof Ident id && scope.var(id.name()) == null) {
            switch (id.name()) {
                case "Math" -> {
                    return math(c.name(), c.args(), scope);
                }
                case "doc" -> {
                    if (c.name().equals("containsKey") && c.args().size() == 1) {
                        return new Sql(column(constantString(c.args().get(0), scope)) != null ? "TRUE" : "FALSE", SqlType.BOOL);
                    }
                    throw refuse("calls doc." + c.name() + ", which is not served");
                }
                case "params" -> {
                    if (c.name().equals("get") && c.args().size() == 1) {
                        return param(constantString(c.args().get(0), scope));
                    }
                    if (c.name().equals("containsKey") && c.args().size() == 1) {
                        return new Sql(params.containsKey(constantString(c.args().get(0), scope)) ? "TRUE" : "FALSE", SqlType.BOOL);
                    }
                    throw refuse("calls params." + c.name() + ", which is not served");
                }
                case "Integer", "Long", "Short", "Byte" -> {
                    return switch (c.name()) {
                        case "parseInt", "parseLong", "valueOf" -> cast("long", one(c, scope));
                        case "toString" -> cast("String", one(c, scope));
                        default -> throw refuse("calls " + id.name() + "." + c.name() + ", which is not served");
                    };
                }
                case "Double", "Float" -> {
                    return switch (c.name()) {
                        case "parseDouble", "parseFloat", "valueOf" -> cast("double", one(c, scope));
                        case "isNaN" -> new Sql("isnan(" + one(c, scope).expr() + ")", SqlType.BOOL);
                        case "isInfinite" -> infinite(one(c, scope));
                        case "toString" -> cast("String", one(c, scope));
                        default -> throw refuse("calls " + id.name() + "." + c.name() + ", which is not served");
                    };
                }
                case "String" -> {
                    if (c.name().equals("valueOf")) {
                        return cast("String", one(c, scope));
                    }
                    throw refuse("calls String." + c.name() + ", which is not served");
                }
                case "Boolean" -> {
                    if (c.name().equals("parseBoolean") || c.name().equals("valueOf")) {
                        return cast("boolean", one(c, scope));
                    }
                    throw refuse("calls Boolean." + c.name() + ", which is not served");
                }
                case "Instant" -> {
                    if (c.name().equals("ofEpochMilli")) {
                        return fromMillis(one(c, scope));
                    }
                    if (c.name().equals("now")) {
                        return fromMillis(new Sql(Long.toString(InfinoClock.read(nowMillis)), SqlType.INT));
                    }
                    throw refuse("calls Instant." + c.name() + ", which is not served");
                }
                case "ZonedDateTime", "LocalDateTime", "OffsetDateTime" -> {
                    if (c.name().equals("ofInstant") && !c.args().isEmpty()) {
                        return emit(c.args().get(0), scope);
                    }
                    if (c.name().equals("now")) {
                        return fromMillis(new Sql(Long.toString(InfinoClock.read(nowMillis)), SqlType.INT));
                    }
                    throw refuse("calls " + id.name() + "." + c.name() + ", which is not served");
                }
                case "System" -> {
                    if (c.name().equals("currentTimeMillis")) {
                        return new Sql(Long.toString(InfinoClock.read(nowMillis)), SqlType.INT);
                    }
                    throw refuse("calls System." + c.name() + ", which is not served");
                }
                case "ZoneId", "ZoneOffset" -> throw refuse("names a time zone; values are read in UTC");
                default -> {
                }
            }
        }
        // ChronoUnit.DAYS.between(a, b)
        if (c.target() instanceof Access unit && unit.target() instanceof Ident id && id.name().equals("ChronoUnit") && c.name().equals("between") && c.args().size() == 2) {
            long per = switch (unit.name()) {
                case "DAYS" -> DAY_MS;
                case "HOURS" -> HOUR_MS;
                case "MINUTES" -> MINUTE_MS;
                case "SECONDS" -> SECOND_MS;
                case "MILLIS" -> 1L;
                default -> throw refuse("uses ChronoUnit." + unit.name() + ", which is not served");
            };
            Sql a = millis(emit(c.args().get(0), scope));
            Sql b = millis(emit(c.args().get(1), scope));
            return new Sql("((" + b.expr() + " - " + a.expr() + ") / " + per + ")", SqlType.INT);
        }
        if (c.target() instanceof Ident id && scope.var(id.name()) == null && Character.isUpperCase(id.name().charAt(0))) {
            // A class the reader does not know (Math and the others are handled above).
            throw refuse("calls " + id.name() + "." + c.name() + ", which is not served");
        }
        Sql target = emit(c.target(), scope);
        return method(target, c.name(), c.args(), scope);
    }

    /** Argument {@code i} of a call, or the refusal naming the call when it is missing. */
    private Sql arg(List<Sql> v, int i, String name) {
        if (i >= v.size()) {
            throw refuse("calls " + name + " with " + v.size() + " arguments; it takes more");
        }
        return v.get(i);
    }

    private Sql one(Call c, Scope scope) {
        if (c.args().size() != 1) {
            throw refuse("calls " + c.name() + " with " + c.args().size() + " arguments; one is read");
        }
        return emit(c.args().get(0), scope);
    }

    /** A bare call: a helper function the script defined, or an expression-language function. */
    private Sql function(String name, List<Expr> args, Scope scope) {
        FuncDef f = scope.func(name);
        if (f != null) {
            if (f.params().size() != args.size()) {
                throw refuse("calls " + name + " with " + args.size() + " arguments; it takes " + f.params().size());
            }
            Scope inner = new Scope(scope);
            for (int i = 0; i < args.size(); i++) {
                inner.vars.put(f.params().get(i), emit(args.get(i), scope));
            }
            Sql out = block(f.body(), inner, null);
            if (out == null) {
                throw refuse("calls " + name + ", which returns nothing");
            }
            return out;
        }
        return switch (name) {
            case "abs", "sqrt", "log", "log10", "log1p", "pow", "min", "max", "round", "floor", "ceil", "exp" -> math(name, args, scope);
            case "isnan" -> new Sql("isnan(" + emit(args.get(0), scope).expr() + ")", SqlType.BOOL);
            case "isinf" -> infinite(emit(args.get(0), scope));
            default -> throw refuse("calls " + name + ", which is not declared");
        };
    }

    private Sql math(String name, List<Expr> args, Scope scope) {
        List<Sql> v = new ArrayList<>();
        for (Expr a : args) {
            v.add(emit(a, scope));
        }
        if (v.isEmpty()) {
            throw refuse("calls Math." + name + " with no argument");
        }
        String x = arg(v, 0, name).expr();
        SqlType t = arg(v, 0, name).type();
        return switch (name) {
            case "abs" -> new Sql("abs(" + x + ")", t.isNumeric() ? t : SqlType.FLOAT);
            case "sqrt" -> new Sql("sqrt(" + x + ")", SqlType.FLOAT);
            case "log" -> new Sql("ln(" + x + ")", SqlType.FLOAT);
            case "log10" -> new Sql("log10(" + x + ")", SqlType.FLOAT);
            case "log1p" -> new Sql("ln(1 + " + x + ")", SqlType.FLOAT);
            case "exp" -> new Sql("exp(" + x + ")", SqlType.FLOAT);
            case "pow" -> new Sql("power(" + x + ", " + arg(v, 1, name).expr() + ")", SqlType.FLOAT);
            case "round" -> new Sql("CAST(round(" + x + ") AS BIGINT)", SqlType.INT);
            case "floor" -> new Sql("floor(" + x + ")", SqlType.FLOAT);
            case "ceil" -> new Sql("ceil(" + x + ")", SqlType.FLOAT);
            case "min", "max" -> {
                List<String> parts = new ArrayList<>();
                boolean integral = true;
                for (Sql s : v) {
                    parts.add(s.expr());
                    integral &= s.type() == SqlType.INT;
                }
                yield new Sql((name.equals("min") ? "least(" : "greatest(") + String.join(", ", parts) + ")", integral ? SqlType.INT : SqlType.FLOAT);
            }
            default -> throw refuse("calls Math." + name + ", which is not served");
        };
    }

    private static Sql infinite(Sql x) {
        return new Sql("(" + x.expr() + " = CAST('inf' AS DOUBLE) OR " + x.expr() + " = CAST('-inf' AS DOUBLE))", SqlType.BOOL);
    }

    /** A method on a value, by the value's type. */
    private Sql method(Sql target, String name, List<Expr> args, Scope scope) {
        List<Sql> v = new ArrayList<>();
        for (Expr a : args) {
            v.add(emit(a, scope));
        }
        String x = target.expr();
        // Methods every value answers.
        switch (name) {
            case "get" -> {
                // A Supplier bound by a helper function: its value.
                return target;
            }
            case "toString" -> {
                return cast("String", target);
            }
            case "equals" -> {
                return new Sql("(" + x + " = " + arg(v, 0, name).expr() + ")", SqlType.BOOL);
            }
            case "hashCode" -> throw refuse("calls hashCode, which is not served");
            default -> {
            }
        }
        if (target.type() == SqlType.STRING) {
            return switch (name) {
                case "length" -> new Sql("length(" + x + ")", SqlType.INT);
                case "toLowerCase" -> new Sql("lower(" + x + ")", SqlType.STRING);
                case "toUpperCase" -> new Sql("upper(" + x + ")", SqlType.STRING);
                case "trim", "strip" -> new Sql("trim(" + x + ")", SqlType.STRING);
                case "contains" -> new Sql("(strpos(" + x + ", " + arg(v, 0, name).expr() + ") > 0)", SqlType.BOOL);
                case "startsWith" -> new Sql("starts_with(" + x + ", " + arg(v, 0, name).expr() + ")", SqlType.BOOL);
                case "endsWith" -> new Sql("ends_with(" + x + ", " + arg(v, 0, name).expr() + ")", SqlType.BOOL);
                case "indexOf" -> new Sql("(strpos(" + x + ", " + arg(v, 0, name).expr() + ") - 1)", SqlType.INT);
                case "isEmpty" -> new Sql("(length(" + x + ") = 0)", SqlType.BOOL);
                case "equalsIgnoreCase" -> new Sql("(lower(" + x + ") = lower(" + arg(v, 0, name).expr() + "))", SqlType.BOOL);
                case "concat" -> new Sql("(" + x + " || " + cast("String", arg(v, 0, name)).expr() + ")", SqlType.STRING);
                case "replace" -> new Sql("replace(" + x + ", " + arg(v, 0, name).expr() + ", " + arg(v, 1, name).expr() + ")", SqlType.STRING);
                case "matches" -> new Sql("regexp_like(" + x + ", " + arg(v, 0, name).expr() + ")", SqlType.BOOL);
                case "substring" -> {
                    // Java counts from 0 with an exclusive end; substr from 1 with a length.
                    String start = arg(v, 0, name).expr();
                    if (v.size() == 1) {
                        yield new Sql("substr(" + x + ", " + start + " + 1)", SqlType.STRING);
                    }
                    yield new Sql("substr(" + x + ", " + start + " + 1, " + arg(v, 1, name).expr() + " - " + start + ")", SqlType.STRING);
                }
                case "charAt" -> new Sql("substr(" + x + ", " + arg(v, 0, name).expr() + " + 1, 1)", SqlType.STRING);
                default -> noSuch(target, name);
            };
        }
        if (target.type().isNumeric()) {
            return switch (name) {
                case "intValue", "longValue" -> cast("long", target);
                case "doubleValue", "floatValue" -> cast("double", target);
                case "getValue" -> target; // a DayOfWeek or Month already read as its number
                case "isNaN" -> new Sql("isnan(" + x + ")", SqlType.BOOL);
                case "isInfinite" -> infinite(target);
                case "compareTo" -> new Sql("(CASE WHEN " + x + " < " + arg(v, 0, name).expr() + " THEN -1 WHEN " + x + " > " + arg(v, 0, name).expr() + " THEN 1 ELSE 0 END)", SqlType.INT);
                default -> noSuch(target, name);
            };
        }
        if (target.type() == SqlType.DATE) {
            return switch (name) {
                case "toInstant", "toZonedDateTime", "toLocalDateTime", "withZoneSameInstant", "atZone" -> target;
                case "toEpochMilli", "getMillis", "getTime" -> millis(target);
                case "getEpochSecond" -> new Sql("(" + millis(target).expr() + " / 1000)", SqlType.INT);
                case "getHour" -> datePart("hour", target);
                case "getMinute" -> datePart("minute", target);
                case "getSecond" -> datePart("second", target);
                case "getDayOfMonth" -> datePart("day", target);
                case "getMonth", "getMonthValue" -> datePart("month", target);
                case "getYear" -> datePart("year", target);
                case "getDayOfYear" -> datePart("doy", target);
                case "getDayOfWeek", "getDayOfWeekEnum" -> new Sql("(((" + datePart("dow", target).expr() + " + 6) % 7) + 1)", SqlType.INT);
                case "getValue" -> target; // DayOfWeek / Month enums already read as numbers
                case "isBefore" -> new Sql("(" + x + " < " + arg(v, 0, name).expr() + ")", SqlType.BOOL);
                case "isAfter" -> new Sql("(" + x + " > " + arg(v, 0, name).expr() + ")", SqlType.BOOL);
                default -> noSuch(target, name);
            };
        }
        return noSuch(target, name);
    }

    private static Sql datePart(String part, Sql date) {
        return new Sql("CAST(date_part('" + part + "', " + date.expr() + ") AS BIGINT)", SqlType.INT);
    }

    /** The cast that makes a date from milliseconds, undone by {@link #millis} so the two cancel. */
    private static final String FROM_MILLIS_PREFIX = "arrow_cast(";
    private static final String FROM_MILLIS_SUFFIX = ", 'Timestamp(Millisecond, None)')";

    /** Epoch milliseconds of a date value; a number is taken as milliseconds already. */
    private Sql millis(Sql v) {
        if (v.type() == SqlType.DATE) {
            String e = v.expr();
            if (e.startsWith(FROM_MILLIS_PREFIX) && e.endsWith(FROM_MILLIS_SUFFIX)) {
                return new Sql(e.substring(FROM_MILLIS_PREFIX.length(), e.length() - FROM_MILLIS_SUFFIX.length()), SqlType.INT);
            }
            return new Sql("arrow_cast(" + e + ", 'Int64')", SqlType.INT);
        }
        if (v.type().isNumeric()) {
            return new Sql(v.expr(), SqlType.INT);
        }
        throw refuse("reads milliseconds of [" + v.expr() + "], which is not a date");
    }

    /** A date from epoch milliseconds; a date stays one. */
    private Sql fromMillis(Sql v) {
        if (v.type() == SqlType.DATE) {
            return v;
        }
        return new Sql(FROM_MILLIS_PREFIX + v.expr() + FROM_MILLIS_SUFFIX, SqlType.DATE);
    }

    private Sql construct(New n, Scope scope) {
        switch (n.className()) {
            case "Date" -> {
                if (n.args().isEmpty()) {
                    return fromMillis(new Sql(Long.toString(InfinoClock.read(nowMillis)), SqlType.INT));
                }
                Sql v = emit(n.args().get(0), scope);
                return fromMillis(v);
            }
            case "StringBuilder", "StringBuffer" -> {
                if (n.args().isEmpty()) {
                    return new Sql("''", SqlType.STRING);
                }
                return cast("String", emit(n.args().get(0), scope));
            }
            default -> throw refuse("constructs a " + n.className() + ", which is not served");
        }
    }

    private Sql cast(String type, Sql v) {
        return switch (type) {
            case "int", "long", "short", "byte" -> v.type() == SqlType.INT
                ? v
                : v.type() == SqlType.FLOAT
                    ? new Sql("CAST(trunc(" + v.expr() + ") AS BIGINT)", SqlType.INT)
                    : new Sql("CAST(" + v.expr() + " AS BIGINT)", SqlType.INT);
            case "double", "float" -> v.type() == SqlType.FLOAT ? v : new Sql("CAST(" + v.expr() + " AS DOUBLE)", SqlType.FLOAT);
            case "String" -> v.type() == SqlType.STRING ? v : new Sql("CAST(" + v.expr() + " AS VARCHAR)", SqlType.STRING);
            case "boolean" -> v.type() == SqlType.BOOL ? v : new Sql("CAST(" + v.expr() + " AS BOOLEAN)", SqlType.BOOL);
            case "def", "Object" -> v;
            default -> throw refuse("casts to " + type + ", which is not served");
        };
    }

    /** A value where a condition is read: a boolean as is, a number as non-zero. */
    private Sql bool(Sql v) {
        if (v.type() == SqlType.BOOL) {
            return v;
        }
        if (v.type().isNumeric()) {
            return new Sql("(" + v.expr() + " <> 0)", SqlType.BOOL);
        }
        if (v.type() == SqlType.UNKNOWN) {
            return new Sql("(" + v.expr() + ")", SqlType.BOOL);
        }
        throw refuse("uses [" + v.expr() + "] as a condition, which is not a boolean");
    }

    private Sql binary(Binary b, Scope scope) {
        Sql l = emit(b.left(), scope);
        Sql r = emit(b.right(), scope);
        switch (b.op()) {
            case "&&" -> {
                return new Sql("(" + bool(l).expr() + " AND " + bool(r).expr() + ")", SqlType.BOOL);
            }
            case "||" -> {
                return new Sql("(" + bool(l).expr() + " OR " + bool(r).expr() + ")", SqlType.BOOL);
            }
            case "==", "!=" -> {
                boolean not = b.op().equals("!=");
                if (b.right() instanceof Null) {
                    return new Sql("(" + l.expr() + (not ? " IS NOT NULL)" : " IS NULL)"), SqlType.BOOL);
                }
                if (b.left() instanceof Null) {
                    return new Sql("(" + r.expr() + (not ? " IS NOT NULL)" : " IS NULL)"), SqlType.BOOL);
                }
                Sql[] sides = align(l, r);
                return new Sql("(" + sides[0].expr() + (not ? " <> " : " = ") + sides[1].expr() + ")", SqlType.BOOL);
            }
            case "<", "<=", ">", ">=" -> {
                Sql[] sides = align(l, r);
                return new Sql("(" + sides[0].expr() + " " + b.op() + " " + sides[1].expr() + ")", SqlType.BOOL);
            }
            case "+" -> {
                if (l.type() == SqlType.STRING || r.type() == SqlType.STRING) {
                    return new Sql("(" + cast("String", l).expr() + " || " + cast("String", r).expr() + ")", SqlType.STRING);
                }
                return arithmetic("+", l, r);
            }
            case "-", "*", "/", "%" -> {
                return arithmetic(b.op(), l, r);
            }
            default -> throw refuse("uses the operator " + b.op() + ", which is not read");
        }
    }

    /** Two sides of a comparison in one type: a date beside a number is read as milliseconds. */
    private Sql[] align(Sql l, Sql r) {
        if (l.type() == SqlType.DATE && r.type().isNumeric()) {
            return new Sql[] { millis(l), r };
        }
        if (r.type() == SqlType.DATE && l.type().isNumeric()) {
            return new Sql[] { l, millis(r) };
        }
        return new Sql[] { l, r };
    }

    private Sql arithmetic(String op, Sql l, Sql r) {
        if (!(l.type().isNumeric() || l.type() == SqlType.UNKNOWN) || !(r.type().isNumeric() || r.type() == SqlType.UNKNOWN)) {
            throw refuse("applies " + op + " to [" + l.expr() + "] and [" + r.expr() + "], which are not both numbers");
        }
        // Integer division stays integer, as in Painless; anything with a double is a double.
        SqlType type = l.type() == SqlType.INT && r.type() == SqlType.INT ? SqlType.INT : SqlType.FLOAT;
        return new Sql("(" + l.expr() + " " + op + " " + r.expr() + ")", type);
    }
}

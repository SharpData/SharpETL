package com.github.sharpdata.sharpetl.spark.extra.flyway.hive;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.resource.Resource;
import org.flywaydb.core.internal.parser.*;
import org.flywaydb.core.internal.sqlscript.Delimiter;
import org.flywaydb.core.internal.sqlscript.ParsedSqlStatement;
import org.flywaydb.core.internal.sqlscript.SqlStatement;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class HiveParser extends Parser {
    protected HiveParser(Configuration configuration, ParsingContext parsingContext, int peekDepth) {
        super(configuration, parsingContext, peekDepth);
    }

    @Override
    protected Delimiter getDefaultDelimiter() {
        return super.getDefaultDelimiter();
    }

    @Override
    protected char getIdentifierQuote() {
        return super.getIdentifierQuote();
    }

    @Override
    protected char getAlternativeIdentifierQuote() {
        return super.getAlternativeIdentifierQuote();
    }

    @Override
    protected char getAlternativeStringLiteralQuote() {
        return super.getAlternativeStringLiteralQuote();
    }

    @Override
    protected char getOpeningIdentifierSymbol() {
        return super.getOpeningIdentifierSymbol();
    }

    @Override
    protected char getClosingIdentifierSymbol() {
        return super.getClosingIdentifierSymbol();
    }

    @Override
    protected Set<String> getValidKeywords() {
        return super.getValidKeywords();
    }

    @Override
    protected boolean supportsPeekingMultipleLines() {
        return super.supportsPeekingMultipleLines();
    }

    @Override
    protected SqlStatement getNextStatement(Resource resource, PeekingReader reader, Recorder recorder, PositionTracker tracker, ParserContext context) {
        return super.getNextStatement(resource, reader, recorder, tracker, context);
    }

    @Override
    protected boolean shouldAdjustBlockDepth(ParserContext context, List<Token> tokens, Token token) {
        return super.shouldAdjustBlockDepth(context, tokens, token);
    }

    @Override
    protected boolean shouldDiscard(Token token, boolean nonCommentPartSeen) {
        return super.shouldDiscard(token, nonCommentPartSeen);
    }

    @Override
    protected void resetDelimiter(ParserContext context) {
        super.resetDelimiter(context);
    }

    @Override
    protected void adjustDelimiter(ParserContext context, StatementType statementType) {
        super.adjustDelimiter(context, statementType);
    }

    @Override
    protected int getTransactionalDetectionCutoff() {
        return super.getTransactionalDetectionCutoff();
    }

    @Override
    protected void adjustBlockDepth(ParserContext context, List<Token> tokens, Token keyword, PeekingReader reader) throws IOException {
        super.adjustBlockDepth(context, tokens, keyword, reader);
    }

    @Override
    protected int getLastKeywordIndex(List<Token> tokens) {
        return super.getLastKeywordIndex(tokens);
    }

    @Override
    protected int getLastKeywordIndex(List<Token> tokens, int endIndex) {
        return super.getLastKeywordIndex(tokens, endIndex);
    }

    @Override
    protected boolean doTokensMatchPattern(List<Token> previousTokens, Token current, Pattern regex) {
        return super.doTokensMatchPattern(previousTokens, current, regex);
    }

    @Override
    protected ParsedSqlStatement createStatement(PeekingReader reader, Recorder recorder, int statementPos, int statementLine, int statementCol, int nonCommentPartPos, int nonCommentPartLine, int nonCommentPartCol, StatementType statementType, boolean canExecuteInTransaction, Delimiter delimiter, String sql) throws IOException {
        return super.createStatement(reader, recorder, statementPos, statementLine, statementCol, nonCommentPartPos, nonCommentPartLine, nonCommentPartCol, statementType, canExecuteInTransaction, delimiter, sql);
    }

    @Override
    protected Boolean detectCanExecuteInTransaction(String simplifiedStatement, List<Token> keywords) {
        return super.detectCanExecuteInTransaction(simplifiedStatement, keywords);
    }

    @Override
    protected String readKeyword(PeekingReader reader, Delimiter delimiter, ParserContext context) throws IOException {
        return super.readKeyword(reader, delimiter, context);
    }

    @Override
    protected String readIdentifier(PeekingReader reader) throws IOException {
        return super.readIdentifier(reader);
    }

    @Override
    protected Token handleDelimiter(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        return super.handleDelimiter(reader, context, pos, line, col);
    }

    @Override
    protected boolean isAlternativeStringLiteral(String peek) {
        return super.isAlternativeStringLiteral(peek);
    }

    @Override
    protected boolean isDelimiter(String peek, ParserContext context, int col, int colIgnoringWhitepace) {
        return super.isDelimiter(peek, context, col, colIgnoringWhitepace);
    }

    @Override
    protected boolean isLetter(char c, ParserContext context) {
        return super.isLetter(c, context);
    }

    @Override
    protected boolean isSingleLineComment(String peek, ParserContext context, int col) {
        return super.isSingleLineComment(peek, context, col);
    }

    @Override
    protected boolean isKeyword(String text) {
        return super.isKeyword(text);
    }

    @Override
    protected boolean isCommentDirective(String peek) {
        return super.isCommentDirective(peek);
    }

    @Override
    protected Token handleCommentDirective(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        return super.handleCommentDirective(reader, context, pos, line, col);
    }

    @Override
    protected Token handleStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        return super.handleStringLiteral(reader, context, pos, line, col);
    }

    @Override
    protected Token handleAlternativeStringLiteral(PeekingReader reader, ParserContext context, int pos, int line, int col) throws IOException {
        return super.handleAlternativeStringLiteral(reader, context, pos, line, col);
    }

    @Override
    protected Token handleKeyword(PeekingReader reader, ParserContext context, int pos, int line, int col, String keyword) throws IOException {
        return super.handleKeyword(reader, context, pos, line, col, keyword);
    }
}

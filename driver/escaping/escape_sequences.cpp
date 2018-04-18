/*
 
 https://docs.faircom.com/doc/sqlref/#33384.htm
 https://docs.microsoft.com/ru-ru/sql/odbc/reference/appendixes/time-date-and-interval-functions
 https://my.vertica.com/docs/7.2.x/HTML/index.htm#Authoring/SQLReferenceManual/Functions/Date-Time/TIMESTAMPADD.htm%3FTocPath%3DSQL%2520Reference%2520Manual%7CSQL%2520Functions%7CDate%252FTime%2520Functions%7C_____43

*/
#include "escape_sequences.h"
#include "lexer.h"
#include <map>

#include <iostream>

using namespace std;

namespace {

const std::map<const std::string, const std::string> fn_convert_map {
    {"SQL_TINYINT", "toUInt8"},
    {"SQL_SMALLINT", "toUInt16"},
    {"SQL_INTEGER", "toInt32"},
    {"SQL_BIGINT",  "toInt64"},
    {"SQL_REAL", "toFloat32"},
    {"SQL_DOUBLE", "toFloat64"},
    {"SQL_VARCHAR", "toString"},
    {"SQL_DATE", "toDate"},
    {"SQL_TYPE_DATE", "toDate"},
    {"SQL_TIMESTAMP", "toDateTime"},
    {"SQL_TYPE_TIMESTAMP", "toDateTime"},
};

const std::map<const Token::Type, const std::string> function_map {
    {Token::ROUND,    "round" },
    {Token::POWER,    "pow"},
    {Token::TRUNCATE, "trunc"},
    {Token::SQRT,     "sqrt" },
    {Token::ABS,      "abs" },
    {Token::CONCAT,   "concat" },
    {Token::CURDATE,   "today" },
    {Token::CURRENT_DATE,   "today" },
    {Token::TIMESTAMPDIFF, "dateDiff" },
    //{Token::TIMESTAMPADD, "dateAdd" },
    { Token::SQL_TSI_QUARTER, "toQuarter" },
    { Token::DAYOFWEEK, "toDayOfWeek" },
    { Token::CAST, "CAST" },
    { Token::LCASE, "lower" }
};

const std::map<const Token::Type, const std::string> literal_map {
    // {Token::SQL_TSI_FRAC_SECOND, ""},
    {Token::SQL_TSI_SECOND, "'second'"},
    {Token::SQL_TSI_MINUTE, "'minute'"},
    {Token::SQL_TSI_HOUR, "'hour'"},
    {Token::SQL_TSI_DAY, "'day'"},
    {Token::SQL_TSI_WEEK, "'week'"},
    {Token::SQL_TSI_MONTH, "'month'"},
    {Token::SQL_TSI_QUARTER, "'quarter'"},
    {Token::SQL_TSI_YEAR, "'year'"},
};

const std::map<const Token::Type, const std::string> timeadd_func_map {
    // {Token::SQL_TSI_FRAC_SECOND, ""},
    {Token::SQL_TSI_SECOND, "addSeconds"},
    {Token::SQL_TSI_MINUTE, "addMinutes"},
    {Token::SQL_TSI_HOUR, "addHours"},
    {Token::SQL_TSI_DAY, "addDays"},
    {Token::SQL_TSI_WEEK, "addWeeks"},
    {Token::SQL_TSI_MONTH, "addMonths"},
    {Token::SQL_TSI_QUARTER, "addQuarters"},
    {Token::SQL_TSI_YEAR, "addYears"},
};

const std::map<const Token::Type, const std::string> extract_func_map{
    { Token::SQL_TSI_SECOND, "toSecond" },
    { Token::SQL_TSI_MINUTE, "toMinute" },
    { Token::SQL_TSI_HOUR, "toHour" },
    { Token::SQL_TSI_DAY, "toDayOfMonth" },
    { Token::SQL_TSI_MONTH, "toMonth" },
    { Token::SQL_TSI_YEAR, "toYear" },
};

string convertFunctionByType(const StringView& typeName) {
    const auto type_name_string = typeName.to_string();
    if (fn_convert_map.find(type_name_string) != fn_convert_map.end())
        return fn_convert_map.at(type_name_string);

    return string();
}

string processIdentOrFunction(const StringView seq, Lexer& lex, bool dospace);

string processFunction(const StringView seq, Lexer& lex) {
    const Token fn(lex.Consume());

    if (fn.type == Token::CONVERT) {
        string result;
        if (!lex.Match(Token::LPARENT))
            return seq.to_string();

        auto num = processIdentOrFunction(seq, lex, false);
        if (num.empty())
            return seq.to_string();
        result += num;

        //while (lex.Match(Token::SPACE)) {}

        if (!lex.Match(Token::COMMA)) {
            return seq.to_string();
        }

        //while (lex.Match(Token::SPACE)) {}

        Token type = lex.Consume();
        if (type.type != Token::IDENT) {
            return seq.to_string();
        }

        string func = convertFunctionByType(type.literal.to_string());

        if (!func.empty()) {
            //while (lex.Match(Token::SPACE)) {}
            if (!lex.Match(Token::RPARENT)) {
                return seq.to_string();
            }
            result = func + "(" + result + ")";
        }

        return result;

    } else if (fn.type == Token::TIMESTAMPADD) {
        string result;
        if (!lex.Match(Token::LPARENT))
            return seq.to_string();

        Token type = lex.Consume();
        if (timeadd_func_map.find(type.type) == timeadd_func_map.end())
            return seq.to_string();
        string func = timeadd_func_map.at(type.type);
        if (!lex.Match(Token::COMMA))
            return seq.to_string();
        auto ramount = processIdentOrFunction(seq, lex, false);
        if (ramount.empty())
            return seq.to_string();

        //while ( lex.Match ( Token::SPACE ) ) {}

        if (!lex.Match(Token::COMMA))
            return seq.to_string();


        auto rdate = processIdentOrFunction(seq, lex, false);
        if (rdate.empty())
            return seq.to_string();

        if (!func.empty()) {
            //while ( lex.Match ( Token::SPACE ) ) {}
            if (!lex.Match(Token::RPARENT)) {
                return seq.to_string();
            }
            result = func + "(" + rdate + ", " + ramount + ")";
        }
        return result;
    } else if (fn.type == Token::EXTRACT) {
        string result;
        if (!lex.Match(Token::LPARENT))
            return seq.to_string();

        // Unit
        const Token unit = lex.Consume();

        // FROM
        const Token from = lex.Consume();
        if (from.type != Token::FROM)
            return seq.to_string();

        // Source
        auto source = processIdentOrFunction(seq, lex, false);
        if (source.empty())
            return seq.to_string();
        result += source;

        auto it = extract_func_map.find(unit.type);
        if (it == extract_func_map.end())
            return seq.to_string();

        string func = it->second;
        if (!func.empty()) {
            //while (lex.Match(Token::SPACE)) {}
            if (!lex.Match(Token::RPARENT))
                return seq.to_string();

            result = func + "(" + result + ")";
        }

        return result;
    } else if (fn.type == Token::CURRENT_TIMESTAMP) {

        Token tok = lex.Peek();
        if (tok.type == Token::LPARENT) {
            while (tok.type != Token::RPARENT) {
                lex.Consume();
                tok = lex.Peek();
            }

            lex.Consume();
        }

        return "now()";
    } else if (fn.type == Token::LOCATE) {
        string result;
        if (!lex.Match(Token::LPARENT))
            return seq.to_string();

        auto needle = processIdentOrFunction(seq, lex, false);
        if (needle.empty())
            return seq.to_string();
        lex.Consume();

        auto haystack = processIdentOrFunction(seq, lex, false);
        if (haystack.empty())
            return seq.to_string();
        lex.Consume();

        auto offset = processIdentOrFunction(seq, lex, false);
        lex.Consume();

        result = "position(" + haystack + "," + needle + ")";
        return result;

    } else if (fn.type == Token::LTRIM) {
        if (!lex.Match(Token::LPARENT))
            return seq.to_string();

        auto param = processIdentOrFunction(seq, lex, false);
        if (param.empty())
            return seq.to_string();
        lex.Consume();
        return "replaceRegexpOne(" + param + ", '^\\\\s+', '')";
    } else if (function_map.find(fn.type) != function_map.end()) {
        string result = function_map.at(fn.type);

        if (!lex.Match(Token::LPARENT))
            return seq.to_string();

        //lex.SetEmitSpaces(true);

        Token tok(lex.Peek());
        result += '(';
        bool dospace = false;
        while (tok.type != Token::RPARENT) {
            if (tok.type == Token::EOS || tok.type == Token::INVALID)
                break;

            result += processIdentOrFunction(seq, lex, dospace);
            tok = lex.Peek();
            dospace = (tok.type != Token::COMMA);
        }
        result += ')';
        lex.Consume(); // Consume RPARENT

        //lex.SetEmitSpaces(false);

        return result;
    }

    return seq.to_string();
}

string processDate(const StringView seq, Lexer& lex) {
    Token data = lex.Consume(Token::STRING);
    if (data.isInvalid()) {
        return seq.to_string();
    } else {
        return string("toDate(") + data.literal.to_string() + ")";
    }
}

string removeMilliseconds(const StringView token) {
    if (token.empty()) {
        return string();
    }

    const char* begin = token.data();
    const char* p = begin + token.size() - 1;
    const char* dot = nullptr;
    const bool quoted = (*p == '\'');
    if (quoted) {
        --p;
    }
    for (; p > begin; --p) {
        if (isdigit(*p)) {
            continue;
        }
        if (*p == '.') {
            if (dot) {
                return token.to_string();
            }
            dot = p;
        } else {
            if (dot) {
                return string(begin, dot) + (quoted ? "'" : "");
            }
            return token.to_string();
        }
    }

    return token.to_string();
}

string processDateTime(const StringView seq, Lexer& lex) {
    Token data = lex.Consume(Token::STRING);
    if (data.isInvalid()) {
        return seq.to_string();
    } else {
        return string("toDateTime(") + removeMilliseconds(data.literal) + ")";
    }
}

string processEscapeSequencesImpl(const StringView seq, Lexer& lex) {
    string result;

    if (!lex.Match(Token::LCURLY)) {
        return seq.to_string();
    }

    while (true) {
        while (lex.Match(Token::SPACE)) {}
        const Token tok(lex.Consume());

        switch (tok.type) {
            case Token::FN:
                result += processFunction(seq, lex);
                break;

            case Token::D:
                result += processDate(seq, lex);
                break;
            case Token::TS:
                result += processDateTime(seq, lex);
                break;

            // End of escape sequence
            case Token::RCURLY:
                return result;

            // Unimplemented
            case Token::T:
            default:
                return seq.to_string();
        }
    };
}

string processIdentOrFunction(const StringView seq, Lexer& lex, bool dospace) {
    const auto token = lex.Peek();
    string result;

    if (token.type == Token::LCURLY) {
        //lex.SetEmitSpaces(false);
        result += processEscapeSequencesImpl(seq, lex);
        //lex.SetEmitSpaces(true);
    } else if (token.type == Token::NUMBER || token.type == Token::IDENT) {
        if (dospace)
            result += ' ';
        result += token.literal.to_string();
        lex.Consume();
    } else if (token.type == Token::EXTRACT ||
               token.type == Token::CONVERT ||
               token.type == Token::TIMESTAMPADD ||
               token.type == Token::CURRENT_TIMESTAMP ||
               function_map.find(token.type) != function_map.end()) {
        if (dospace)
            result += ' ';
        //lex.SetEmitSpaces(false);
        result += processFunction(seq, lex);
        //lex.SetEmitSpaces(true);
    } else if (token.type == Token::LPARENT) {
        lex.Consume();
        result += '(';
        Token tok = lex.Peek();
        bool dospace = false;
        while (tok.type != Token::RPARENT) {
            if (tok.type == Token::EOS || tok.type == Token::INVALID)
                break;

            result += processIdentOrFunction(seq, lex, dospace);
            tok = lex.Peek();
            dospace = (tok.type != Token::COMMA);
        }
        result += ')';
        lex.Consume();
    } else {
        if (dospace)
            result += ' ';
        if (literal_map.find(token.type) != literal_map.end())
            result += literal_map.at(token.type);
        else
            result += token.literal.to_string();
        lex.Consume();
    }

    return result;
}

string processEscapeSequences(const StringView seq) {
    Lexer lex(seq);
    lex.SetEmitSpaces(false);
    return processEscapeSequencesImpl(seq, lex);
}

} // namespace

std::string replaceEscapeSequences(const std::string & query)
{
    const char* p = query.c_str();
    const char* end = p + query.size();
    const char* st = p;
    int level = 0;
    std::string ret;

    while (p != end) {
        switch (*p) {
            case '{':
                if (level == 0) {
                    if (st < p) {
                        ret += std::string(st, p);
                    }
                    st = p;
                }
                level++;
                break;

            case '}':
                if (level == 0) {
                    // TODO unexpected '}'
                    return query;
                }
                if (--level == 0) {
                    ret += processEscapeSequences(StringView(st, p + 1));
                    st = p + 1;
                }
                break;
        }

        ++p;
    }

    if (st < p) {
        ret += std::string(st, p);
    }

    return ret;
}

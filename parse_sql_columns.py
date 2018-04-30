""" Parse SQL statements and extract key-value pairs of columns """
from collections import OrderedDict as odict
import unittest
import logging
import json

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML, Token
 
def split_queries(str_in):
    items = sqlparse.parse(str_in)
    return [extract_col_values(q) for q in items if not str(q).isspace()]

def extract_col_values(items):
    r = odict([('query', str(items).strip())])
    if items.get_type() == 'UNKNOWN':
        logging.error("ERROR during parsing of %s", repr(str(items)))
        r['error'] = 'Parsing failed'
    elif items.get_type() != 'INSERT':
        logging.info("not an insert query %s %s", items, items.get_type())
        r['error'] = 'Unsupported query type %s' % items.get_type()
    else:
        # we can have several of those in an "insert all" query
        t_table_and_columns_arr = []
        t_values_tuple_arr = []
        for item in items:
            if isinstance(item, sqlparse.sql.Function):
                t_table_and_columns_arr.append(item)
            elif isinstance(item, sqlparse.sql.Parenthesis):
                t_values_tuple_arr.append(item)
        
        col_value_pairs = []
        for t_table_and_columns, t_values_tuple in zip(t_table_and_columns_arr, t_values_tuple_arr):
            col_value_pairs += tokenize_cols_values(t_table_and_columns, t_values_tuple)
        r['columns'] = col_value_pairs
    return r
    
def tokenize_cols_values(t_cols, t_values_tuple):
    logging.debug("t_cols = %s", t_cols)
    logging.debug("t_values_tuple= %s", t_values_tuple)
    
    t_table, t_columns = [tok for tok in t_cols if isinstance(tok, sqlparse.sql.TokenList)]
    table = str(t_table)
    logging.debug("table = %s", table)
    logging.debug("t_columns = %s", t_columns)
    
    t_column_identifiers = [tok for tok in t_columns if isinstance(tok, sqlparse.sql.IdentifierList)][0]
    logging.debug("t_column_identifiers = %s", t_column_identifiers)
      
    columns = []
    for t in t_column_identifiers:
        str_t = str(t)
        if isinstance(t, Identifier) or t.ttype is Keyword:
            logging.debug("\tfound column %s", str_t)
            columns.append(str_t)
        else:
            logging.debug("\tdiscard token %s", str_t)
    logging.debug("columns = %s", columns)
    
    for t in t_values_tuple:
        logging.debug('\t%s %s %s %s', type(t), type(t).__name__, t.ttype, t)
    t_values = [tok for tok in t_values_tuple if isinstance(tok, sqlparse.sql.IdentifierList)][0]
    values = []
    for t in t_values:
        logging.debug('\t%s %s %s %s', type(t), type(t).__name__, t.ttype, t)
        str_t = str(t)
        if t.ttype in (Token.Punctuation, Token.Text.Whitespace ):
            logging.debug("\tdiscard token %s", str_t)
            continue
        logging.debug("\tstore value %s", str_t)
        values.append( str_t )
    
    r = tuple(zip(["%s.%s" % (table, col) for col in columns], values))
    logging.info(r)
    assert len(values) == len(columns)
    return r

def main(stream):
    print json.dumps(split_queries(stream), indent=4)


class unittests(unittest.TestCase):
    def test_types(self):
        input = "insert into dual(col_str, col_seq, col_int, col_dbl) values('foo',myseq.nextval, 42, 123.456);"
        expected = [{  "query": input,
            "columns": [
                ["dual.col_str", "'foo'"],
                ["dual.col_seq", "myseq.nextval"],
                ["dual.col_int", "42"],
                ["dual.col_dbl", "123.456"]
            ]
        }]
        res = split_queries(input)
        self.assertEqual(json.dumps(res),json.dumps(expected))
        
    def test_insert_all(self):
        input = """insert all 
            into t1(col_a, col_b) values ('v_a', 2)
            into t2(col_d, col_c) values (4, 3);"""
        expected = [{
            "query": input,
            "columns": [
                ["t1.col_a", "'v_a'"],
                ["t1.col_b", "2"],
                ["t2.col_d", "4"],
                ["t2.col_c", "3"]
            ]
        }]
        res = split_queries(input)
        self.assertEqual(json.dumps(res),json.dumps(expected))

    def test_subquery(self):
        input = "insert into t1(q,r) (select count(1), 42 from dual);"
        expected = [{
            "query":input,
            "columns": [
                ["t1.q","count(1)"],
                ["t1.r","42"]
            ]
        }]
        res = split_queries(input)
        self.assertEqual(json.dumps(res),json.dumps(expected))
    
    def test_error_parsing(self):
        input = "reiteb5yiure"
        expected = [{
            "query":input,
            "error":"Parsing failed"
        }]
        res = split_queries(input)
        self.assertEqual(json.dumps(res),json.dumps(expected))

    def test_error_not_update(self):
        input = "select 1 from dual;"
        expected = [{
            "query":input, 
            "error":"Unsupported query type SELECT"
        }]
        res = split_queries(input)
        self.assertEqual(json.dumps(res),json.dumps(expected))

    def test_function(self):
        input = "insert into t1(col1,col2) values(to_date('2018-04-30','YYYY-MM-DD'),systimestamp) 1 from dual;"
        expected = [{
            "query":input,
            "columns": [
                ["t1.col1","to_date('2018-04-30','YYYY-MM-DD')"],
                ["t1.col2","systimestamp"]
            ]
        }]
        res = split_queries(input)
        self.assertEqual(json.dumps(res),json.dumps(expected))

    def test_several_queries(self):
        input = "select 1 from dual;insert into t1(col_a, col_b) values ('v_a', 2);insert into t2(col_c, col_d) values (3, 4);"
        expected = [{
            "query": "select 1 from dual;",
            "error": "Unsupported query type SELECT"
        },{ "query": "insert into t1(col_a, col_b) values ('v_a', 2);",
            "columns": [
                ["t1.col_a", "'v_a'"],
                ["t1.col_b", "2"]
            ]
        },{ "query": "insert into t2(col_c, col_d) values (3, 4);",
            "columns": [
                ["t2.col_c", "3"],
                ["t2.col_d", "4"]
            ]
        }]
        res = split_queries(input)
        self.assertEqual(json.dumps(res),json.dumps(expected))

if __name__ == '__main__':
    import sys
    logging.basicConfig(
           level=logging.DEBUG  if "-vv" in sys.argv 
            else logging.INFO   if "-v"  in sys.argv 
            else logging.ERROR )
    main(sys.stdin.read())
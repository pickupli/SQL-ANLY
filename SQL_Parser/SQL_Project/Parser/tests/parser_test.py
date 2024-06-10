import unittest
from Parser.sql_util import sql_analyzer


class ParserTest(unittest.TestCase):
    def test_parser(self):
        # 测试数据
        demo_sql = 'SELECT SUM(x.aaa) as sumA ,x.bbb AS fstC ,(tbl1.ccc+tbl2.ttt)*tbl3.www, tbl2.ddd ,tbl3.ggg FROM tbl0 x  JOIN tbl1  JOIN tbl2 JOIN tbl3  WHERE x.caa=2 and x.cbb=tbl1.ccc and tbl1.cee=(select sum(abc) from tbl3 where eee=199) and x.cff = (select a4 from tbl4 where id4=123 )   group by x.fff HAVING AVG(tbl1.zzz)>900 order by x.rrr'
        
        self.assertIsNone(sql_analyzer.rereat_info_from_sql(demo_sql))


if __name__ == '__main__':
    unittest.main()
import unittest
from Parser.sql_util import sql_analyzer

class ParserTest(unittest.TestCase):
    def test_parser(self):
        # 测试数据
        demo_sql = []
        demo_sql.append('SELECT SUM(x.aaa) as sumA ,x.bbb AS fstC ,(tbl1.ccc+tbl2.ttt)*tbl3.www, tbl2.ddd ,tbl3.ggg FROM tbl0 x  JOIN tbl1  JOIN tbl2 JOIN tbl3  WHERE x.caa=2 and x.cbb=tbl1.ccc and tbl1.cee=(select sum(abc) from tbl3 where eee=199) and x.cff = (select a4 from tbl4 where id4=123 )   group by x.fff HAVING AVG(tbl1.zzz)>900 order by x.rrr') 
        demo_sql.append('SELECT SUM(x.aaa) as sumA ,x.bbb AS fstC ,(tbl1.ccc+tbl2.ttt)*tbl3.www, tbl2.ddd ,tbl3.ggg FROM tbl0 x  JOIN tbl1  JOIN tbl2 JOIN tbl3  WHERE x.caa=2 and x.cbb=tbl1.ccc and tbl1.cee=(select sum(abc) from tbl3 where eee=199) and x.cff = (select a4 from tbl4 where id4=123 )   group by x.fff HAVING AVG(tbl1.zzz)>900 order by x.rrr')
        demo_sql.append("SELECT t.abb,(t.erf+t2.fff) AS fstC,t2.ggg   FROM (select tbl0.abc as abb  ,tbl1.etf as erf,(tbl1.cc+tbl0.uuu) as ccc from tbl0,tbl1 where tbl0.aaa=102) t,tbl2 t2,tbl3 where t2.ddd=4 and t.ccc=t2.ddd and t2.eee>100 and t2.aa2>(select sum(sss) from tbl4 where id>1000) ")
        demo_sql.append("select tbl0.abc as abb,tbl1.etf as erf,(tbl1.cc+tbl0.uuu) as ccc from tbl0,tbl1 where tbl0.aaa=102 and ccc>1000")
        demo_sql.append('SELECT tbl0.a,tbl0.b,tbl0.c,tbl0.d,bt1.aa  FROM tbl0,(select aa,bb from tbl1 where cc=1) bt1   WHERE g=2 and c=(select  a1 from tbl1 where b2=3)   group by fff  HAVING AVG(zzz)>900 order by rrr')
        demo_sql.append('SELECT tbl0.a,bt1.aa,bt1.bb,tbl2.aaa  FROM tbl0 ,(select aa,bb from tbl1 where cc=1) bt1,tbl2 WHERE tbl0.d=bt1.bb and  bt1.bb=tbl2.ccc and  tbl0.g=2 and tbl0.c=(select  a1 from tbl1 where b2=3)  group by tbl0.fff  HAVING AVG(tbl0.zzz)>900 order by tbl0.rrr')
        demo_sql.append('SELECT a,b,c  FROM tbl0  where d=1 and e=2 and f=(select aa from tbl1 where bb=3)  group by tbl0.f  HAVING AVG(tbl0.g)>900 order by tbl0.h')
        demo_sql.append('SELECT a,b,count(*) as COUNT  FROM tbl0  where d=1 and e=2 and f=(select aa from tbl1 where bb=3)  group by tbl0.f  HAVING AVG(tbl0.g)>900 order by COUNt')
        demo_sql.append("SELECT product_name FROM products WHERE category = 'Electronics' UNION SELECT product_id FROM products WHERE aera = 'Clothing' ORDER BY product_name;")
        demo_sql.append("WITH CategoryCTE AS (SELECT * FROM Category where cat_id>100),ProductCTE AS ( SELECT p.*,cte.CategoryName FROM Product p    INNER JOIN CategoryCTE cte ON p.CategoryID = cte.CategoryID) SELECT ProductCTE.*,CategoryCTE.* FROM ProductCTE,CategoryCTE")
        demo_sql.append("WITH cte AS(SELECT * FROM Product where ProductID>100) SELECT ProductID,ProductCode,ProductName,UnitPrice FROM cte")
        demo_sql.append("SELECT Product.*,Customer.customer_id FROM Product,Customer  where Product.ProductID>100")
        for index in range(0,len(demo_sql)-1):
            info = sql_analyzer.rereat_info_from_sql(demo_sql[index])
            print('SQL-TXT:',demo_sql[index])
            print('TBLS:',info.ref_tbl_set)
            print('OUT:',info.out_col_dict)
            print('CRITERIA:',info.criteria_col_dict)
            print('------------------------------------------------------------')
            self.assertIsNotNone(info)

if __name__ == '__main__':
    unittest.main() 
@Grab('mysql:mysql-connector-java:6.0.3')

import groovy.sql.Sql
import com.mysql.cj.jdbc.MysqlDataSource

def sql = new Sql(new MysqlDataSource().each {
              it.url = 'jdbc:mysql://192.168.1.2:3306/ms?useSSL=false'
              it.user = 'ms'
              it.password = 'spiderdt'
          })


println(sql.rows('select project from ms.report')*.project.unique())
println(sql.rows('select category from ms.report')*.category.unique())
println(sql.rows('select report from ms.report')*.report.unique())
println(sql.rows('select selector from ms.report')*.selector.unique())
println(sql.rows('select selector_desc from ms.report')*.selector_desc.unique())
println(sql.rows('select json from ms.report')*.json.unique())


sql.close()



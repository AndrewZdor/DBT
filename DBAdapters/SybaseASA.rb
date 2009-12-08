require "yaml"

require File.dirname(__FILE__) + '/DBAdapter.rb'

class SybaseASA

  CONFIG = YAML.load_file("#{PROJECT_LOC}/config.yaml")

  def initialize
    @adapter = DBAdapter.new
  end

  def execute_sql(sql) #override

    dbisql_10_path = CONFIG["dbisql_10_path"]
    dbisql_11_path = CONFIG["dbisql_11_path"]

    #conn_str = Ini.read('..\\CoreForms\\config.ini', 'dbURL')[0].to_s.scan(Regexp.new("UID=.*"))[0].to_s
    conn_str = 'UID=dba;PWD=sql'

    dbisql = 'dbisql -c "UID=dba;PWD=sql;DBN=teplograd;ENG=Teploserver;LINKS=SHMem" -codepage 1251 '
    #dbisql   = dbisql_11_path + 'dbisql -c "' + conn_str + '" -codepage 1251 '
    dbisql10 = dbisql_10_path + 'dbisql -c "' + conn_str + '" -codepage 1251 '

    ret_val = system dbisql + sql
    return ret_val

  end

  def db_create
    puts "db create ..."
  end

  def table_create
    adapter.execute_sql("CALL util_dropAllObjects('All')")
    puts 'drop all objects complited'

    adapter.execute_sql("#{PROJECT_LOC}/db/model/teplograd.sql")
    puts "table creation complited"
    puts ""
  end

  def trigger_create
    trigger_path = "#{PROJECT_LOC}/db/src/triggers"

    adapter.execute_sql("CALL util_dropAllObjects('Triggers')")
    puts 'Удалил триггеры'

    Dir.foreach(trigger_path) do |x|
      if File.extname(x) == ".sql" then
        adapter.execute_sql('"' + trigger_path + '\\' + x + '"')
      end
    end

    puts "Создал триггеры "
    puts ""
  end

  def method_missing method
    if @adapter.respond_to? method
      @adapter.send method
    else
      raise NotImplementedError, "This method is not " + \
      "available on this interface"
    end
  end


end
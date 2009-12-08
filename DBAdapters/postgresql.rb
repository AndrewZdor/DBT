require 'yaml'
require 'ftools'
require 'tempfile'

require File.dirname(__FILE__) + '/DBAdapter.rb'

class Postgresql

  PSQL_OPTIONS = ' -h localhost -p 5432 --username ' + CONFIG['username']
  SERVICE_NAME = CONFIG["service_name"]

  def initialize
    @adapter = DBAdapter.new
    ENV['PG_HOME'] ||= CONFIG['PG_HOME']
    ENV['PATH'] = ENV['PG_HOME'] + '\bin;' + ENV['PATH']
    ENV['PGCLIENTENCODING'] ||= CONFIG["encoding"]
    ENV['PGUSER']     ||= CONFIG["username"]
    ENV['PGHOST']     ||= CONFIG["host"]
    ENV['PGPORT']     ||= CONFIG["port"].to_s
    ENV['PGPASSWORD'] ||= CONFIG["password"].to_s
  end

  # Executes sql.
  # 'errLevel' argument applied to textual(non-file) sql.
  def execute_sql(sql, stopOnError = true, errLevel = 'warning', isAtomic = true)

    if File.file?(sql) then
      file = File.open(sql, 'r')
    else # create temp file and populate it with special commands.
      file = Tempfile.new('execute_sql')
      sql = "SET client_min_messages TO #{errLevel};\n" + sql
      file.print(sql)
      file.flush
    end
    puts "execute_sql on: #{file.path}\n"

    psql_options = PSQL_OPTIONS
    psql_options += ' --file "' + file.path + '" '
    psql_options += ' -1 ' if isAtomic
    psql_options += ' -v ON_ERROR_STOP=1 ' if stopOnError

    psql = "psql.exe #{psql_options} #{CONFIG["database"]}"
    puts psql

    result = system(psql)
    if !result
      if file.class.name == 'Tempfile'
        tmpPath = file.path
        errorPath = File.join(BUILD_PATH, 'error_' + File.basename(tmpPath))
        File.copy(tmpPath, errorPath)
      end
      raise ("Error executing psql:\n    #{psql}")
    end
    return result
  end

  def db_console
  end

  def db_create(db_name)
    # Create database
    create_db = "createdb.exe #{PSQL_OPTIONS} --encoding #{CONFIG["encoding"]} --echo #{db_name}"
    puts create_db
    if !system(create_db)
      raise "Error: could not create DB!"
    end

    execute_sql("ALTER USER #{CONFIG["username"]} SET client_min_messages TO 'WARNING'")

    # Create PROCEDURAL LANGUAGE 'plpgsql'
    sql = "DROP LANGUAGE IF EXISTS plpgsql CASCADE;
      CREATE LANGUAGE 'plpgsql';
      ALTER LANGUAGE plpgsql OWNER TO #{CONFIG["username"]};"
    execute_sql(sql)

    # Create CITEXT extension in the pg_catalog schema.
    citextSQL = IO.read("#{ENV['PG_HOME']}/share/contrib/citext.sql")
    citextSQL.gsub!(/SET search_path = public;/, 'SET search_path = pg_catalog;')
    execute_sql(citextSQL, true, 'warning')

    # Create uuid-ossp extension in the pg_catalog schema.
    uuidSQL = IO.read("#{ENV['PG_HOME']}/share/contrib/uuid-ossp.sql")
    citextSQL.gsub!(/SET search_path = public;/, 'SET search_path = pg_catalog;')
    execute_sql(uuidSQL, true, 'warning')

    # Enable pl/pgsql dedbugger for created database.
    # Assuming that shared_preload_libraries option
    # in postgresql.conf set to '$libdir/plugins/plugin_debugger.dll'
    sql = "SET search_path = public; \n"
    sql += IO.read("#{ENV['PG_HOME']}/share/contrib/pldbgapi.sql")
    execute_sql(sql)

  end

  def db_drop(db_name)
    # Drop connections.
    system('net stop ' + SERVICE_NAME)
    system('net start ' + SERVICE_NAME)

    drop_db = 'dropdb.exe ' + PSQL_OPTIONS + ' --echo ' + db_name
    puts drop_db
    return system(drop_db)
  end

  def db_structure_dump(dump_file)
    dump_structure = '"' + ENV['PG_HOME'] + '\bin\pg_dump.exe" ' + PSQL_OPTIONS + ' --format plain --schema-only --no-owner --file "' + dump_file + '" ' + CONFIG['database']
    puts dump_structure
    ret_val = system dump_structure
    return ret_val
  end

  def db_repair(dump_file)
    repair_db = '"' + ENV['PG_HOME'] + '\bin\pg_restore.exe" ' + PSQL_OPTIONS + ' -i -d "' + CONFIG['database'] + '" -c -v "' + dump_file + '"'
    puts repair_db
    ret_val = system repair_db
    return ret_val
  end

  def db_archive_restore(from_file, to_path)
    # stop db service
    if !system "net stop #{CONFIG['service_name']}" # if service is already stopped - continue anyway.
      puts "Service is already stopped - continue anyway."
    end

    # delete old db
    puts "Deleting old db - #{to_path}"
    Dir.new(to_path).each do |x|
      if x[/./] != '.'
        begin
          puts "deleting #{x}"
          FileUtils.rm_rf(to_path + "/#{x}")
        rescue Exception
          raise "#{x} can not be deleted. Task terminated!"
        end
      end
    end
    puts "...Ok"

    puts "Copying archived db to folder: #{to_path} ..."
    if system "#{CONFIG["winrar"]} x -ac -ibck #{from_file} #{to_path}/"
      puts "...Ok"
    else
      raise "Error when unraring!"
    end

    # start db service
    if !(system "net start " + CONFIG["service_name"])
        raise "DB service #{CONFIG["service_name"]} cannot be started. Task terminated!"
    end
    puts "...service started OK."
  end

  def user_create
    execute_sql("#{PROJECT_LOC}/db/src/users/users.sql")
  end

  def user_drop
  end

  def domain_create
    execute_sql("#{PROJECT_LOC}/db/src/domains/domains.sql")
  end

  def table_create
    execute_sql("#{PROJECT_LOC}/db/src/tables/Tables.sql")
  end

  def fkey_create
    execute_sql("#{PROJECT_LOC}/db/src/fkeys/fkeys.sql")
  end

  def index_create
    execute_sql("#{PROJECT_LOC}/db/src/indexes/indexes.sql")
  end

  def trigger_create
    execute_sql("#{PROJECT_LOC}/db/src/triggers/triggers.sql")
  end

  def data_load

    data_path = "#{PROJECT_LOC}/db/model/Data/unload/"
    Dir.foreach(data_path) do |x|
      if File.fnmatch('*.dat', x) then
        str = open(data_path + x).read.gsub(/\357\273\277/, '')
        open(data_path + x, 'w') { |f| f.write(str) }
      end
    end

    tempFile = Tempfile.new("load_data")

    str = open("#{PROJECT_LOC}/db/migrate/data/data.sql").read.
      gsub(/%WORK_PATH/, "#{PROJECT_LOC}")

    tempFile.puts str
    tempFile.close()
    execute_sql(tempFile.path)
    tempFile.unlink()
  end

  def method_missing (method)
    if @adapter.respond_to? method
      @adapter.send method
    else
      raise NotImplementedError,
        "This method #{method} is not available on this interface"
    end
  end

  def vacuumAnalyze (tableName = '')
    sql = 'VACUUM FULL ANALYZE '
    if !(tableName == nil or tableName.empty)
      sql += tableName
    end
    execute_sql(sql, true, 'warning', false)
  end

end
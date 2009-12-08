Rake.application.options.trace = true

require 'tempfile'
require 'ftools'
require 'yaml'
require 'erb'
require 'RubyLib'

CURRENT_DIR = File.dirname(__FILE__)
CONFIG_FILE_NAME = '/config/database.yml'

project_loc = MyLib.fixPath(ENV['project_loc'])
if project_loc === nil or project_loc.empty? \
    or !File.exist?(project_loc + CONFIG_FILE_NAME) \
    or project_loc == CURRENT_DIR
  project_loc = MyLib.fixPath(ENV['default_project_loc'])
end
if project_loc == nil or project_loc.empty?
  raise 'Nor <project_loc>, nor <default_project_loc> env variables not set.'
end

PROJECT_LOC = project_loc
#CONFIG = YAML.load_file(PROJECT_LOC + CONFIG_FILE_NAME)
env = ENV['RAILS_ENV'] || 'development'
unless CONFIG = YAML::load(ERB.new(IO.read(PROJECT_LOC + CONFIG_FILE_NAME)).result)[env]
  abort "No database is configured for the environment '#{env}'"
end

ENV['PGDATA']  ||= CONFIG['PGDATA']

BUILD_PATH = PROJECT_LOC + '/db/build'
PROC_PATH = PROJECT_LOC + '/db/src/procedures'
PROC_BUILD_FILE = BUILD_PATH + '/proc.sql'
VIEW_BUILD_FILE = BUILD_PATH + '/view.sql'
DEPLOY_FILE = PROJECT_LOC + '/db/migrate/migrate.sql'
PATCH_FILE = PROJECT_LOC + '/db/src/patch.sql'
SCRIPT_PATH = PROJECT_LOC + '/db/src/scripts'

# Initializing DB adapter
adapterName = CONFIG["adapter"]
require CURRENT_DIR + '/DBAdapters/' + adapterName
DB_ADAPTER = eval(adapterName.capitalize).new

# Load and execute project's specific rake file.
local_rakefile = PROJECT_LOC + '/rakefile.rb'
load(local_rakefile) if File.exist?(local_rakefile)

desc "Default task"
task :default do |t|
  puts 'Default task'
end

namespace :db do

  desc 'Call interactive db console'
  task :console do
    DB_ADAPTER.db_console
  end

  desc 'Create database'
  task :create, [:db_name] do |t, args|
    args.with_defaults(:db_name => CONFIG['database'])
    DB_ADAPTER.db_create(args.db_name)
    puts 'Created OK'
  end

  desc 'Drop database'
  task :drop, [:db_name] do |t, args|
    args.with_defaults(:db_name => CONFIG['database'])
     if !DB_ADAPTER.db_drop(args.db_name)
       raise 'Error dropping DB!'
     end
    puts 'Dropped OK'
  end

  desc "Obfuscate database"
  task :obfuscate do |t|
    puts "Obfuscate Database"
    DB_ADAPTER.execute_sql(SCRIPT_PATH + '/obfuscateDB.sql')
    puts "Obfuscate Database Complited"
    puts ""
  end

  desc "bootstrap"
  task :bootstrap => [:create, "domain:create", "table:create", "data:load",
    "fkey:create", "index:create", "trigger:create", "view:create", "migrate:patch", "structure:dump", :vacuumAnalyze] do
  end

  namespace :domain do
    desc "Create domain(s)"
    task :create do |t|
        DB_ADAPTER.domain_create()
    end
  end

  namespace :data do
    desc "Load data"
    task :load do |t|
        DB_ADAPTER.data_load()
    end
  end

  namespace :structure do
    desc "Dump the database structure to a SQL file"
    task :dump, [:dump_file] do |t, args|
    args.with_defaults(:dump_file => "#{PROJECT_LOC}/db/src/#{CONFIG['database']}.sql")
      DB_ADAPTER.db_structure_dump(args.dump_file)
    end
  end

  namespace :dump do
    desc "Load dump"
    task :load, [:dump_file] do |t, args|
      args.with_defaults(:dump_file => CONFIG["dump_file"])
      DB_ADAPTER.db_repair(args.dump_file)
    end
  end

  namespace :archive do
    desc "Restore DB archive"
    task :restore, [:from_file, :to_path] do |t, args|
      args.with_defaults(:from_file => "#{PROJECT_LOC}/#{CONFIG["db_archive_file"]}", :to_path => ENV['PGDATA'])
      DB_ADAPTER.db_archive_restore(args.from_file, args.to_path)
    end
  end

  namespace :user do
    desc "Create user(s)"
    task :create do |t|
      DB_ADAPTER.user_create
    end

    desc "Drop user(s)"
    task :drop do |t|
      DB_ADAPTER.user_drop
    end
  end

  namespace :table do
    desc "Create table(s)"
    task :create do |t|
      DB_ADAPTER.table_create()
    end

    namespace :audit_column do
      desc "Add Audit Columns(InsertTS, UpdateTS, InsertUserId, UpdateUserId, Revision) to all tables"
      task :create do |t|
        DB_ADAPTER.execute_sql("#{SCRIPT_PATH}/addAuditColumns.sql")
        puts "addition of audit columns complited"
      end

      desc "Drop Audit Columns(InsertTS, UpdateTS, InsertUserId, UpdateUserId, Revision) to all tables"
      task :drop do |t|
        DB_ADAPTER.execute_sql("#{SCRIPT_PATH}/removeAuditColumns.sql")
        puts "deletion of audit columns complited"

      end
    end
  end

  namespace :view do
    desc "Create view(s)"
    task :create do |t|
      view_body = ''

      FileList[File.join("#{PROJECT_LOC}/db/src/views", "*.sql")].each do |view| # View loop.
        view_body += open(view).read + "\n\n\n\n" # such a neat delimiter
      end

      File.open(VIEW_BUILD_FILE, "w") { |file| file.syswrite(view_body) }

      DB_ADAPTER.execute_sql(VIEW_BUILD_FILE)
      puts "creation of view(s) complited"
    end
  end

  namespace :fkey do
    desc "Create foreign key(s)"
    task :create do |t|
      DB_ADAPTER.fkey_create
    end
  end

  namespace :index do
    desc "Create index(es)"
    task :create do |t|
      DB_ADAPTER.index_create
    end
  end

  namespace :trigger do
     desc "Create triggers"
     task :create => "proc:create" do |t|
       DB_ADAPTER.trigger_create
     end
   end

  def generateBody (procPath)
    # TODO: Add 'dropIfExists' and 'grantExecute' as it was in prepareProc method.
    # TODO: Parse /** ... */ comment at the beginning and generate procedure comments.
    return open(procPath).read
  end

  def remove_sql_rem(str)
    res = str.gsub(/(?im)\/\*\*.*?\*\//, '') # Remove multiline comments.
    res = res.gsub(/\/\/.*/, '').gsub(/--.*/, '') # Remove single line comments.
    res
  end

  # Creates dependencies tree of tasks for procedure compilation.
  def calc_dependents
  	Dir.mkdir(BUILD_PATH) unless File.directory?(BUILD_PATH)
    # Re-create proc file.
    File.delete(PROC_BUILD_FILE) if File.exist?(PROC_BUILD_FILE)
    File.new(PROC_BUILD_FILE, "w")
    File.open(PROC_BUILD_FILE, "a") do |file| # append
      file.syswrite("SET client_min_messages TO warning;\n")
      file.syswrite("\n")
      drop_all_proc = open(SCRIPT_PATH + '/dropAllProc.sql').read + "\n\n"
      drop_all_proc["%username"] = CONFIG["username"]
      file.syswrite(drop_all_proc)
    end

    FileList[File.join(PROC_PATH, "*.sql")].each do |src| # Proc loop.
      # TODO: Check if declared function name corresponds to the file name (consider comments).
      task src do |t|
        procBody = generateBody(src)
        File.open(PROC_BUILD_FILE, "a") do |file| # append
          file.syswrite(procBody)
          file.syswrite("\n\n\n\n") # such a neat delimiter
        end
      end
      task :generate_file => src;
    end

    # Add dependencies between procedures.
    taskList = Rake::Task[:generate_file].prerequisites
    taskList.each do |pre| # Prerequicite procedures loop.
      procName = pre[/\w+(?=\.sql)/]
      taskList.each do |dep| # Dependent procedures loop.
        next if dep == pre # exclude the same procedure
        # TODO: Optimize by making a hash of {proc=>Content} at the previous stage.
        procContent = remove_sql_rem(open(dep).read)
        task dep => pre if procContent.include?(procName) # The soul of the task!!!
      end
    end
  end #calc_dependents


  #Generate file for procedure creation.
  task :generate_file => calc_dependents()


  namespace :proc do

#    desc "Create procedures"
    task :create => :generate_file do |t|
      puts "Executing proc file..."
      DB_ADAPTER.execute_sql(PROC_BUILD_FILE)
      puts "Procedures creation complited"
    end
  end

  namespace :migrate do
    revision = open(PATCH_FILE).read[/\$.*?\$/]
    patch_sql = "SELECT util_db_migrate('begin', '#{revision}');\n\n" +
                open(PATCH_FILE).read +
                "\nSELECT util_db_migrate('end', '#{revision}');"

    desc "Generate migration file"
    task :file => :generate_file do |t|

      # http://help.eclipse.org/stable/index.jsp?topic=/org.eclipse.pde.doc.user/tasks/pde_product_build.htm

      # To run the build you will use the org.elipse.ant.core.antRunner application. When invoking eclipse with this application to perform a build you need to set two arguments on the command line:
      # -buildfile </path/to/productBuild.xml>:  This is the path to the productBuild.xml provided by pde build.  It is located in the org.eclipse.pde.build/scripts/productBuild directory.  This is the build file that drives the whole product build process.
      # -Dbuilder=</path/to/configuration folder>:  This is the path to the build configuration folder.

      # Run the antRunner application using the following command:
      #java -jar <eclipseInstall>/plugins/org.eclipse.equinox.launcher_<version>.jar -application org.eclipse.ant.core.antRunner -buildfile <<eclipseInstall>/plugins/org.eclipse.pde.build_<version>/scripts/productBuild/productBuild.xml> -Dbuilder=<path to the build configuration folder>

      str = ''
      str += "\n\n/*\n===============================================================\n"
      str += " Applying latest patch\n"
      str += "===============================================================\n*/\n\n"
      str += patch_sql

      str += "\n\n/*\n===============================================================\n"
      str += " Recreating all procedures\n"
      str += "===============================================================\n*/\n\n"
      str += open(PROC_BUILD_FILE).read

      str += "\n\n/*\n===============================================================\n"
      str += " Recreating all views\n"
      str += "===============================================================\n*/\n\n"
      str += open(VIEW_BUILD_FILE).read

      str += "\n\n/*\n===============================================================\n"
      str += " Obfuscating DB\n"
      str += "===============================================================\n*/\n\n"
      str += open(SCRIPT_PATH + '\\obfuscateDB.sql').read

      open(DEPLOY_FILE, "w") { |o| o.puts str }

      puts "Deploy Complited"
    end

    desc "reattach virgin DB"
    task :reattachDB do |t|
      # stop db service
      system "net stop " + CONFIG["service_name"] # if service is already stopped - continue anyway.

      # delete dirty db file
      dbPath = CONFIG["DBFolder"] + '\\' + CONFIG["DBFile"]
      puts "Deleting dirty db file #{dbPath}"
      begin
          FileUtils.rm dbPath
      rescue Exception
          puts "DB file #{dbPath} canot be deleted. Task terminated!"
          raise
      end
      puts "...Ok"

      # delete old log file
      logPath = CONFIG["DBFolder"] + '\\teplograd.log'
      puts "Deleting delete old log file #{logPath}"
      begin
          if File::exists?(logPath)
            FileUtils.rm logPath
          end
      rescue Exception
          puts "Log file #{logPath} canot be deleted. Task terminated!"
          raise
      end
      puts "...Ok"

      puts "Copying virgin db file to db folder: #{dbPath}"
      #copy virgin db to DB folder
      FileUtils.cp "#{PROJECT_LOC}\\db\\model\\teplograd.db", dbPath
      puts "...Ok"

      # start db service
      if !(system "net start " + CONFIG["service_name"])
          puts "DB service #{CONFIG["service_name"]} cannot be started. Task terminated!"
          raise
      end
      puts "...service started OK."
    end

    desc "run patch over new db"
    task :patch do |t|
      DB_ADAPTER.execute_sql("#{PROC_PATH}/util_db_migrate.sql")
      puts 'Paching db...'
      puts revision
      if DB_ADAPTER.execute_sql(patch_sql)
        puts "Patched Ok"
      end
    end

  end # namespace :migrate

  desc "run patch over new db, recreate procedures, view and structure dump"
  task :migrate => ["archive:restore", "migrate:patch", "proc:create", "view:create", "structure:dump"] do
  end

  desc 'Runs Vacuum and Analyze on given table or entire DB.'
  task :vacuumAnalyze, [:tableName] do |t, args|
    DB_ADAPTER.vacuumAnalyze(args.tableName)
    puts 'Vacuumed and Analyzed OK'
  end

end # namespace :db


# Override task's execute method - add descriptive header.
class Rake::Task
    alias execute_old execute
    def execute (args=nil)
        _ = "*" * 80
        puts "\n#{_}\n* task: #{name} (#{comment})\n#{_}"
        #system 'chcp 1251'
        execute_old
    end
end

#def prepareProc(procFile1, proc_path1, SCRIPT_PATH1)
#  if procFile1 != 'tmp'
#    str = open(SCRIPT_PATH1 + '\\dropAllProc.sql').read + "\n\n"
#    #open(procFile1, "w") { |o| o.puts open(SCRIPT_PATH1 + '\\dropAllProc.sql').read + "\n\n" }
#
#    Dir.foreach(proc_path1) do |x|
#      if File.fnmatch('*.sql', x) then
#        str += open(SCRIPT_PATH1 + '\\dropIfExists.sql').read.gsub(/%ProcName/, File.basename(x, '.sql')) + "\n\n"
#            str += open(proc_path1 + '\\' + x).read + "\n\n"
#            str += open(SCRIPT_PATH1 + '\\grantExecute.sql').read.gsub(/%ProcName/, File.basename(x, '.sql')) + "\n\n" if File.fnmatch('rcp*.sql', x)
#        endS
#    end
#
#    return str
#    #open(procFile1, "w") { |o| o.puts str }
#  end
#end


desc 'Create YAML test fixtures from data in an existing database.
Defaults to development database.  Set RAILS_ENV to override.'

require 'Ya2YAML'
require 'rubygems'
require 'active_record'

task :extract_fixtures do
  sql  = "SELECT * FROM %s"
  skip_tables = ["schema_info"]
  ActiveRecord::Base.establish_connection(CONFIG[ENV['DB'] || 'development'])
  (ActiveRecord::Base.connection.tables - skip_tables).each do |table_name|
    i = "000"
    #File.open("#{RAILS_ROOT}/test/fixtures/#{table_name}.yml", 'w') do |file|
    File.open("#{PROJECT_LOC}/test/fixtures/#{table_name}.yml", 'w') do |file|
      data = ActiveRecord::Base.connection.select_all(sql % table_name)
      file.write data.inject({}) { |hash, record|
        hash["#{table_name}_#{i.succ!}"] = record
        hash
      }.ya2yaml
    end
  end
end
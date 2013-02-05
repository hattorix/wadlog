cluster = require 'cluster'
syslog  = require 'node-syslog'
opts    = require 'opts'
wadlog  = require './wadlog'

# Azure account
config = require './config'
process.env['AZURE_STORAGE_ACCOUNT']    = config.azureAccount.name
process.env['AZURE_STORAGE_ACCESS_KEY'] = config.azureAccount.key

# Parse command-line options
options = [
  { short       : 'f'
  , long        : 'follow'
  , description : 'output appended data as the log grows'
  },
  { long        : 'from'
  , description : 'from date...'
  , value       : true
  },
  { long        : 'to'
  , description : 'to date...'
  , value       : true
  },
  { short       : 'c'
  , long        : 'condition'
  , description : 'table select condition'
  , value       : true
  },
  { short       : 's'
  , long        : 'sleep-interval'
  , description : 'with -f, sleep S seconds between iterations'
  , value       : true
  },
]
opts.parse options, true

# ログの行に対する処理
syslog.init "wadlog", syslog.LOG_PID | syslog.LOG_ODELAY, syslog.LOG_LOCAL3
getLevel = (sourceLevel) ->
  switch sourceLevel
    when 1 # System.Diagnostics.TraceEventType.Critical
      syslog.LOG_CRIT
    when 2 # System.Diagnostics.TraceEventType.Error
      syslog.LOG_ERR
    when 3 # System.Diagnostics.TraceEventType.Warning
      syslog.LOG_WARNING
    when 4 # System.Diagnostics.TraceEventType.Information
      syslog.LOG_INFO
    else # System.Diagnostics.TraceEventType.Verbose
      syslog.LOG_DEBUG
rowFunction = (entity) ->
  date = wadlog.ticksToDate entity.EventTickCount
  level = getLevel entity.Level
  syslog.log level, "#{date} [#{entity.RoleInstance}] #{entity.Message}"

# query オブジェクトを生成する
columns = ['EventTickCount', 'Level', 'Role', 'RoleInstance', 'Message']
createQuery = (from, to, condition) ->
  ticks = wadlog.getTicks from
  query = wadlog
    .select(columns...)
    .where('PartitionKey >= ?', "0#{ticks}")
    .and("EventTickCount >= #{ticks}L")
  if to?
    toTicks = wadlog.getTicks to
    query
      .and('PartitionKey < ?', '0' + (toTicks + 300000))
      .and("EventTickCount <= #{toTicks}L")
  if condition? then query.and condition else query

# log を tail -f する関数
wadlogTailf = (from, interval, condition = null) ->
  execQuery = (lastRow, query) ->
    if lastRow?
      # 前回取得した最終行の次から取得するクエリを生成
      query = wadlog.next lastRow, columns
      query.and condition if condition?

    wadlog.queryAll query, rowFunction, (l, q) -> setTimeout execQuery, interval, l, q
  execQuery null, createQuery(from, null, condition)

from      = if opts.get 'from' then new Date opts.get('from') else new Date(new Date().getTime() - 300000)
condition = opts.get 'c'
to        = new Date opts.get('to') if opts.get 'to'

# メイン関数
mainProc = ->
  if opts.get('f') and not to?
    interval = if opts.get 's' then opts.get('s') * 1000  else 15000
    wadlogTailf from, interval, condition
  else
    wadlog.queryAll createQuery(from, to, condition), rowFunction
      , -> process.exit()

if config.clusterRoles?
  # クラスタ実行
  if cluster.isMaster
    # 親プロセス
    for role in config.clusterRoles
      worker = cluster.fork()
      worker.send role

  else
    # 子プロセス
    process.on 'message', (msg) ->
      # タグ名をロール名で上書き
      syslog.init msg, syslog.LOG_PID | syslog.LOG_ODELAY, syslog.LOG_LOCAL3

      condition += " and " if condition? and condition != ""
      condition = (condition ? '') + "Role == '#{msg}'"
      mainProc()

else
  # 単独プロセスで実行
  mainProc()

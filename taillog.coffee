opts   = require 'opts'
wadlog = require './wadlog'

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
rowFunction = (entity) ->
  date = wadlog.ticksToDate entity.EventTickCount
  console.log "#{date} [#{entity.RoleInstance}] #{entity.Message}"

# query オブジェクトを生成する
columns = ['EventTickCount', 'Level', 'Role', 'RoleInstance', 'Message']
createQuery = (from, condition) ->
  ticks = wadlog.getTicks from
  query = wadlog
    .select(columns...)
    .where('PartitionKey > ?', "0#{ticks}")
    .and("EventTickCount > #{ticks}L")
  if condition? then query.and condition else query

# log を tail -f する関数
wadlogTailf = (from, interval, condition = null) ->
  execQuery = (lastRow, query) ->
    if lastRow?
      # 前回取得した最終行の次から取得するクエリを生成
      query = wadlog.next lastRow, columns
      query.and condition if condition?

    wadlog.queryAll query, rowFunction, (l, q) -> setTimeout execQuery, interval, l, q
  execQuery null, createQuery(from, condition)

from      = if opts.get 'from' then new Date opts.get('from') else new Date(new Date().getTime() - 300000)
condition = opts.get 'c'

if opts.get 'f'
  interval = if opts.get 's' then opts.get('s') * 1000  else 15000
  wadlogTailf from, interval, condition
else
  wadlog.queryAll createQuery(from, condition), rowFunction

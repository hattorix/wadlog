azure = require 'azure'

# Unix epoch の Ticks
EPOCH = 621355968000000000

# Unix epoch to Ticks
exports.getTicks = (date) ->
  (date.getTime() * 10000) + EPOCH

# Ticks to Unix epoch
exports.ticksToDate = (ticks) ->
  offset = (ticks - EPOCH) / 10000
  new Date(offset)

# 配列 array に 'PartitionKey' が存在することを保証する
sumPartitionKey = (array) ->
  hasPartitionKey = false
  # copy array
  tmp = []
  for item in array
    hasPartitionKey = true if not hasPartitionKey and item == 'PartitionKey'
    tmp.push item
  # 'PartitionKey' がなかったら足す
  tmp.push 'PartitionKey' if not hasPartitionKey
  return tmp

# base query
exports.select = () ->
  columns = sumPartitionKey arguments
  azure.TableQuery
    .select(columns...)
    .from('WADLogsTable')

# lastEntity より新しい行を取得するクエリを生成します
exports.next = (lastEntity, columns) ->
  columns = sumPartitionKey columns
  azure.TableQuery
    .select(columns...)
    .from('WADLogsTable')
    .where('PartitionKey >= ?', lastEntity.PartitionKey)

# query にマッチするすべての行を取得
exports.queryAll = (query, process, finished = null) ->
  tableService = azure.createTableService()
  lastEntity = null
  retry = 0
  retrySleep = [10000, 20000, 40000, 80000, 160000, 320000]

  QueryCallback = (error, entities, options) ->
    if error
      if retry < retrySleep.length
        setTimeout ->
          # なぜか以下のエラーになることがあるので、tableService を再生成
          # - Error: One of the request inputs is not valid.
          tableService = azure.createTableService()
          tableService.queryEntities query, QueryCallback
        , retrySleep[retry]
        retry += 1
        console.error "[Request failed (Retry #{retry}/#{retrySleep.length})] #{error}"
      else
        console.error "[Gave up...] #{error}"
        console.error query
      return

    if entities?
      for entity in entities
        # 行ごとの処理
        process entity
      lastEntity = entities.pop()

    if options.nextPartitionKey?
      # 継続行があるなら繰り返す
      query.whereNextKeys options.nextPartitionKey, options.nextRowKey
      tableService.queryEntities query, QueryCallback

    else if finished?
      # 最終行まで終わったらコールバック
      finished lastEntity, query

  tableService.queryEntities query, QueryCallback

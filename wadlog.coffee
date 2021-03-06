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

# base query
exports.select = () ->
  columns = ['PartitionKey', 'RowKey']
  for arg in arguments
    columns.push arg
  azure.TableQuery
    .select(columns...)
    .from('WADLogsTable')

# lastEntity より新しい行を取得するクエリを生成します
exports.next = (lastEntity, columns) ->
  tmp = columns.concat ['PartitionKey', 'RowKey']
  azure.TableQuery
    .select(tmp...)
    .from('WADLogsTable')
    .where('PartitionKey >= ?', lastEntity.PartitionKey)
    .and('RowKey != ?', lastEntity.RowKey)
    .whereNextKeys(lastEntity.PartitionKey, lastEntity.RowKey)

# query にマッチするすべての行を取得
exports.queryAll = (query, process, finished = null) ->
  tableService = azure.createTableService()
  lastEntity = null

  QueryCallback = (error, entities, options) ->
    if error
      console.error error
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

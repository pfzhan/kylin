import { pageSizeMapping } from '../../../../config'

export function getDatabaseTree (databases) {
  return databases.map(databaseName => ({
    id: databaseName,
    label: databaseName,
    children: [],
    type: 'database',
    isMore: false,
    isLoading: true,
    isSelected: false,
    pagination: {
      pageOffset: 0,
      pageSize: pageSizeMapping.TABLE_TREE
    },
    render: (h, { node, data, store }) => {
      const { label } = data
      return (
        <div class="database">
          <div class="label">{label}</div>
          <div class="select-all" onClick={event => this.handleClickNode(data, null, event, true)}>
            {data.isSelected ? this.$t('cleanAll') : this.$t('selectAll')}
          </div>
        </div>
      )
    }
  }))
}

export function getTableTree (database, res, isTableReset) {
  const newTables = res.tables.map(table => ({
    id: `${database.id}.${table.table_name}`,
    label: table.table_name,
    type: 'table',
    database: database.id,
    isSelected: table.loaded,
    clickable: !table.loaded,
    isLoaded: table.loaded,
    render: (h, { node, data, store }) => {
      const isChecked = data.isLoaded || data.isSelected
      return (
        <div>
          { isChecked ? (
            <span class="el-icon-ksd-good_health"></span>
          ) : null}
          <span>{data.label}</span>
        </div>
      )
    }
  }))
  database.children = isTableReset
    ? newTables
    : database.children.concat(newTables)
  database.isMore = res.size > getChildrenCount(database)
  database.isLoading = false
}

function getChildrenCount (data) {
  return data.children.filter(data => !['isMore', 'isLoading'].includes(data.type)).length
}

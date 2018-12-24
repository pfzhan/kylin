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
          <div class="label">
            {data.isSelected ? (
              <span class="el-icon-ksd-good_health"></span>
            ) : null}
            {label}
          </div>
          <div class="select-all" onClick={event => this.handleSelectDatabase(event, data)}>
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
    isSelected: table.loaded || database.isSelected,
    clickable: !table.loaded && !database.isSelected,
    isLoaded: table.loaded,
    render: (h, { node, data, store }) => {
      const isChecked = !data.isLoaded && data.isSelected
      const isLoaded = data.isLoaded
      const isAllTableSelected = database.children.filter(item => item.type !== 'isMore').every(item => item.isLoaded)
      const tableClassNames = [
        'table',
        ...(database.isSelected && !isAllTableSelected ? ['parent-selected'] : []),
        ...(!data.clickable ? ['disabled'] : []),
        ...(isLoaded ? ['synced'] : [])
      ]
      const currentId = `table-load-${data.id}`
      setTimeout(() => {
        const currentEl = document.getElementById(currentId)
        const parentEl = currentEl && currentEl.parentNode.parentNode.parentNode
        if (parentEl && !data.clickable) {
          parentEl.style.backgroundColor = 'transparent'
          parentEl.style.cursor = 'not-allowed'
        } else {
          parentEl.style.backgroundColor = null
          parentEl.style.cursor = null
        }
      })
      return (
        <div>
          <div class={tableClassNames} id={currentId}>
            { isChecked ? (
              <span class="el-icon-ksd-good_health"></span>
            ) : null }
            <span>{data.label}</span>
          </div>
          { isLoaded ? (
            <span class="label-synced">{this.$t('synced')}</span>
          ) : null }
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

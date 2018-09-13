import { sourceTypes, sourceNameMapping } from '../../../config'
import { getColumnMockTags } from './mock'

export const render = {
  datasource: {
    render (h, { node, data, store }) {
      const { sourceType, project, label } = data

      return (
        <div class="datasource font-medium">
          <span>{label}</span>
          <div class="right">
            { this.isShowLoadSource ? (
              <i class="tree-icon table-action el-icon-ksd-add_table" onClick={event => this.loadDataSource(sourceType, project, event)}></i>
            ) : null}
          </div>
        </div>
      )
    }
  },
  database: {
    render (h, { node, data, store }) {
      const { label } = data

      return (
        <div class="database font-medium">
          <div class="left">
            <i class="tree-icon el-icon-ksd-data_source"></i>
          </div>
          <span>{label}</span>
        </div>
      )
    }
  },
  table: {
    render (h, { node, data, store }) {
      const { label, tags, dateRange } = data

      return (
        <div class="table font-medium">
          <div class="left">
            {tags.map(tag => {
              switch (tag) {
                case 'F':
                  return <i class="tree-icon el-icon-ksd-fact_table"></i>
                case 'L':
                  return <i class="tree-icon el-icon-ksd-lookup_table"></i>
                case 'N':
                default:
                  return <i class="tree-icon el-icon-ksd-sample"></i>
              }
            })}
          </div>
          <span title={label}>{label}</span>
          <div class="right">
            { dateRange ? (
              <el-popover
                placement="right"
                title="Data Range:"
                trigger="hover"
                content={dateRange}>
                <i class="tree-icon table-date-tip el-icon-ksd-data_range" slot="reference"></i>
              </el-popover>
            ) : null }
          </div>
        </div>
      )
    }
  },
  column: {
    render (h, { node, data, store }) {
      const { label, tags } = data

      return (
        <div class="column">
          <div class="left">
            {tags.map(tag => {
              switch (tag) {
                case 'FK':
                  return <i class="tree-icon column-tag el-icon-ksd-symbol_fk"></i>
                case 'PK':
                  return <i class="tree-icon column-tag el-icon-ksd-symbol_pk"></i>
              }
            })}
          </div>
          <span title={label}>{label}</span>
        </div>
      )
    }
  }
}

export function sortDatasource (datasourceArray) {
  datasourceArray.forEach(datasource => {
    datasource.children.sort((itemA, itemB) => itemA.label > itemB.label ? 1 : -1)
    datasource.children.forEach(database => {
      database.children.sort((itemA, itemB) => itemA.label > itemB.label ? 1 : -1)
      database.children.forEach(table => {
        table.children.sort((itemA, itemB) => itemA.label > itemB.label ? 1 : -1)
      })
    })
  })
  return datasourceArray
}

export function getDatasourceTree (that, tableDatas, project) {
  const datasourceArray = []

  tableDatas.forEach(table => {
    const sourceType = table.source_type
    const datasource = datasourceArray.find(datasource => datasource.sourceType === sourceType)

    if (datasource) {
      const database = datasource.children.find(database => database.label === table.database)

      if (database) {
        database.children.push(getTableObj(that, table))
      } else {
        datasource.children.push(getDatabaseObj(that, table))
      }
    } else {
      datasourceArray.push(getDatasourceObj(that, table, project))
    }
  })

  return sortDatasource(datasourceArray)
}

export function getDatasourceObj (that, table, project) {
  const sourceType = table.source_type
  const datasourceType = sourceTypes[sourceType]
  const datasourceName = sourceNameMapping[datasourceType]

  return {
    id: `datasouce-Source: ${datasourceName}`,
    label: `Source: ${datasourceName}`,
    render: render.datasource.render.bind(that),
    children: [
      getDatabaseObj(that, table)
    ],
    sourceType,
    project,
    type: 'datasource'
  }
}

function getDatabaseObj (that, table) {
  const sourceType = table.source_type
  const datasourceType = sourceTypes[sourceType]
  const datasourceName = sourceNameMapping[datasourceType]

  return {
    id: `database-${table.database}`,
    label: table.database,
    render: render.database.render.bind(that),
    children: [
      getTableObj(that, table)
    ],
    type: 'database',
    datasource: datasourceName
  }
}

function getTableObj (that, table) {
  const sourceType = table.source_type
  const datasourceType = sourceTypes[sourceType]
  const datasourceName = sourceNameMapping[datasourceType]
  const tags = [
    ...(table.root_fact ? ['F'] : []),
    ...(table.lookup ? ['L'] : []),
    ...(!table.root_fact && !table.lookup ? ['N'] : [])
  ]
  const dateRange = table.start_time && table.end_time ? getDateRangeStr(table) : null

  return {
    id: `table-${table.name}`,
    label: table.name,
    children: getColumnObjArray(that, table.columns),
    render: render.table.render.bind(that),
    tags,
    type: 'table',
    database: table.database,
    datasource: datasourceName,
    dateRange
  }
}

function getColumnObjArray (that, columnArray) {
  return columnArray.map(column => ({
    id: `column-${column.name}`,
    label: column.name,
    render: render.column.render.bind(that),
    tags: getColumnMockTags() || [],
    type: 'column'
  }))
}

export function getAutoCompleteWords (datasourceTree) {
  let autoCompleteWords = []
  datasourceTree.forEach(datasource => {
    autoCompleteWords = [
      ...autoCompleteWords,
      ...getChildrenWords(datasource)
    ]
  })
  return autoCompleteWords
}

function getChildrenWords (parent) {
  let { children } = parent
  let words = []

  if (children.length) {
    children.forEach(child => {
      words.push({
        meta: child.type,
        caption: child.label,
        value: child.label,
        scope: 1
      })

      if (child.type === 'table') {
        words.push({
          meta: child.type,
          caption: `${parent.label}.${child.label}`,
          value: `${parent.label}.${child.label}`,
          scope: 1
        })
      }

      if (child.children) {
        words = [...words, ...getChildrenWords(child)]
      }
    })
  }

  return words
}

function getDateRangeStr (table) {
  const startDate = new Date(table.start_time)
  const endDate = new Date(table.end_time)

  return `${getDateStr(startDate)} to ${getDateStr(endDate)}`
}

function getDateStr (date) {
  const year = date.getFullYear()
  const month = getDoubleNumber(date.getMonth() + 1)
  const day = getDoubleNumber(date.getDate())
  return `${year}-${month}-${day}`
}

function getDoubleNumber (number) {
  return number < 10 ? `0${number}` : String(number)
}

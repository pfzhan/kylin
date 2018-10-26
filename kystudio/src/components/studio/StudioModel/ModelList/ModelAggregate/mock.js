export const aggregateGroups = [
  {
    includes: ['KYLIN_ACCOUNT.ACCOUNT_ID', 'KYLIN_SALES.SELLER_ID'],
    select_rule: {
      hierarchy_dims: [],
      mandatory_dims: [],
      joint_dims: [],
      dim_cap: 0
    }
  },
  {
    includes: ['KYLIN_ACCOUNT.ACCOUNT_ID', 'KYLIN_SALES.SELLER_ID'],
    select_rule: {
      hierarchy_dims: [],
      mandatory_dims: ['KYLIN_ACCOUNT.ACCOUNT_ID'],
      joint_dims: []
    }
  },
  {
    includes: ['KYLIN_SALES.SELLER_ID'],
    select_rule: {
      hierarchy_dims: [],
      mandatory_dims: ['KYLIN_SALES.SELLER_ID'],
      joint_dims: []
    }
  }
]

// export const flowerJSON = [{
//   name: 'a',
//   size: 2000,
//   children: [{
//     name: 'b',
//     size: 2000,
//     children: [{
//       name: 'c',
//       size: 2000,
//       children: [{
//         name: 'd',
//         size: 2000,
//         children: [{
//           name: 'e',
//           size: 2000,
//           children: [{
//             name: 'f',
//             size: 2000,
//             children: []
//           }]
//         }]
//       }]
//     }]
//   }, {
//     name: 'g',
//     size: 2000,
//     children: [{
//       name: 'h',
//       size: 2000,
//       children: [{
//         name: 'i',
//         size: 2000,
//         children: [{
//           name: 'j',
//           size: 2000,
//           children: [{
//             name: 'k',
//             size: 2000
//           }]
//         }]
//       }]
//     }]
//   }, {
//     name: 'l',
//     size: 2000,
//     children: [{
//       name: 'm',
//       size: 2000,
//       children: [{
//         name: 'n',
//         size: 2000,
//         children: [{
//           name: 'j',
//           size: 2000,
//           children: [{
//             name: 'k',
//             size: 2000
//           }]
//         }]
//       }]
//     }]
//   }]
// }]
export const flowerJSON = [{
  name: 'flare',
  size: 2025,
  children: [
    {
      name: 'analytics',
      size: 250,
      children: [
        {
          name: 'cluster',
          size: 250,
          children: [{
            name: 'cluster1',
            size: 2500,
            children: [{
              name: 'cluster2',
              size: 5500,
              children: [{
                name: 'cluster3',
                size: 7500,
                children: [{
                  name: 'cluster4',
                  size: 10000,
                  children: [{
                    name: 'cluster5',
                    size: 15000
                  }]
                }]
              }]
            }]
          }]
        },
        {
          name: 'graph',
          size: 250,
          children: [
            { name: 'BetweennessCentrality', size: 3534 },
            { name: 'LinkDistance', size: 5731 },
            { name: 'MaxFlowMinCut', size: 7840 },
            { name: 'ShortestPaths', size: 5914 },
            { name: 'SpanningTree', size: 3416 }
          ]
        },
        {
          name: 'optimization',
          size: 250,
          children: [{ name: 'AspectRatioBanker', size: 7074 }]
        }
      ]
    },
    {
      name: 'animate',
      size: 250,
      children: [
        { name: 'Easing', size: 17010 },
        { name: 'FunctionSequence', size: 5842 },
        {
          name: 'interpolate',
          size: 250,
          children: [
            { name: 'ArrayInterpolator', size: 1983 },
            { name: 'ColorInterpolator', size: 2047 },
            { name: 'DateInterpolator', size: 1375 },
            { name: 'Interpolator', size: 8746 },
            { name: 'MatrixInterpolator', size: 2202 },
            { name: 'NumberInterpolator', size: 1382 },
            { name: 'ObjectInterpolator', size: 1629 },
            { name: 'PointInterpolator', size: 1675 },
            { name: 'RectangleInterpolator', size: 2042 }
          ]
        },
        { name: 'ISchedulable', size: 1041 },
        { name: 'Parallel', size: 5176 },
        { name: 'Pause', size: 449 },
        { name: 'Scheduler', size: 5593 },
        { name: 'Sequence', size: 5534 },
        { name: 'Transition', size: 9201 },
        { name: 'Transitioner', size: 19975 },
        { name: 'TransitionEvent', size: 1116 },
        { name: 'Tween', size: 6006 }
      ]
    },
    {
      name: 'data',
      size: 250,
      children: [
        {
          name: 'converters',
          size: 250,
          children: [
            { name: 'Converters', size: 721 },
            { name: 'DelimitedTextConverter', size: 4294 },
            { name: 'GraphMLConverter', size: 9800 },
            { name: 'IDataConverter', size: 1314 },
            { name: 'JSONConverter', size: 2220 }
          ]
        },
        { name: 'DataField', size: 1759 },
        { name: 'DataSchema', size: 2165 },
        { name: 'DataSet', size: 586 },
        { name: 'DataSource', size: 3331 },
        { name: 'DataTable', size: 772 },
        { name: 'DataUtil', size: 3322 }
      ]
    },
    {
      name: 'display',
      size: 250,
      children: [
        { name: 'DirtySprite', size: 8833 },
        { name: 'LineSprite', size: 1732 },
        { name: 'RectSprite', size: 3623 },
        { name: 'TextSprite', size: 10066 }
      ]
    },
    {
      name: 'flex',
      size: 250,
      children: [{ name: 'FlareVis', size: 4116 }]
    },
    {
      name: 'physics',
      size: 250,
      children: [
        { name: 'DragForce', size: 1082 },
        { name: 'GravityForce', size: 1336 },
        { name: 'IForce', size: 319 },
        { name: 'NBodyForce', size: 10498 },
        { name: 'Particle', size: 2822 },
        { name: 'Simulation', size: 9983 },
        { name: 'Spring', size: 2213 },
        { name: 'SpringForce', size: 1681 }
      ]
    },
    {
      name: 'query',
      size: 250,
      children: [
        { name: 'AggregateExpression', size: 1616 },
        { name: 'And', size: 1027 },
        { name: 'Arithmetic', size: 3891 },
        { name: 'Average', size: 891 },
        { name: 'BinaryExpression', size: 2893 },
        { name: 'Comparison', size: 5103 },
        { name: 'CompositeExpression', size: 3677 },
        { name: 'Count', size: 781 },
        { name: 'DateUtil', size: 4141 },
        { name: 'Distinct', size: 933 },
        { name: 'Expression', size: 5130 },
        { name: 'ExpressionIterator', size: 3617 },
        { name: 'Fn', size: 3240 },
        { name: 'If', size: 2732 },
        { name: 'IsA', size: 2039 },
        { name: 'Literal', size: 1214 },
        { name: 'Match', size: 3748 },
        { name: 'Maximum', size: 843 },
        {
          name: 'methods',
          size: 250,
          children: [
            { name: 'add', size: 593 },
            { name: 'and', size: 330 },
            { name: 'average', size: 287 },
            { name: 'count', size: 277 },
            { name: 'distinct', size: 292 },
            { name: 'div', size: 595 },
            { name: 'eq', size: 594 },
            { name: 'fn', size: 460 },
            { name: 'gt', size: 603 },
            { name: 'gte', size: 625 },
            { name: 'iff', size: 748 },
            { name: 'isa', size: 461 },
            { name: 'lt', size: 597 },
            { name: 'lte', size: 619 },
            { name: 'max', size: 283 },
            { name: 'min', size: 283 },
            { name: 'mod', size: 591 },
            { name: 'mul', size: 603 },
            { name: 'neq', size: 599 },
            { name: 'not', size: 386 },
            { name: 'or', size: 323 },
            { name: 'orderby', size: 307 },
            { name: 'range', size: 772 },
            { name: 'select', size: 296 },
            { name: 'stddev', size: 363 },
            { name: 'sub', size: 600 },
            { name: 'sum', size: 280 },
            { name: 'update', size: 307 },
            { name: 'variance', size: 335 },
            { name: 'where', size: 299 },
            { name: 'xor', size: 354 },
            { name: '_', size: 264 }
          ]
        },
        { name: 'Minimum', size: 843 },
        { name: 'Not', size: 1554 },
        { name: 'Or', size: 970 },
        { name: 'Query', size: 13896 },
        { name: 'Range', size: 1594 },
        { name: 'StringUtil', size: 4130 },
        { name: 'Sum', size: 791 },
        { name: 'Variable', size: 1124 },
        { name: 'Variance', size: 1876 },
        { name: 'Xor', size: 1101 }
      ]
    },
    {
      name: 'scale',
      size: 250,
      children: [
        { name: 'IScaleMap', size: 2105 },
        { name: 'LinearScale', size: 1316 },
        { name: 'LogScale', size: 3151 },
        { name: 'OrdinalScale', size: 3770 },
        { name: 'QuantileScale', size: 2435 },
        { name: 'QuantitativeScale', size: 4839 },
        { name: 'RootScale', size: 1756 },
        { name: 'Scale', size: 4268 },
        { name: 'ScaleType', size: 1821 },
        { name: 'TimeScale', size: 5833 }
      ]
    },
    {
      name: 'util',
      size: 250,
      children: [
        { name: 'Arrays', size: 8258 },
        { name: 'Colors', size: 10001 },
        { name: 'Dates', size: 8217 },
        { name: 'Displays', size: 12555 },
        { name: 'Filter', size: 2324 },
        { name: 'Geometry', size: 10993 },
        {
          name: 'heap',
          size: 250,
          children: [
            { name: 'FibonacciHeap', size: 9354 },
            { name: 'HeapNode', size: 1233 }
          ]
        },
        { name: 'IEvaluable', size: 335 },
        { name: 'IPredicate', size: 383 },
        { name: 'IValueProxy', size: 874 },
        {
          name: 'math',
          size: 250,
          children: [
            { name: 'DenseMatrix', size: 3165 },
            { name: 'IMatrix', size: 2815 },
            { name: 'SparseMatrix', size: 3366 }
          ]
        },
        { name: 'Maths', size: 17705 },
        { name: 'Orientation', size: 1486 },
        {
          name: 'palette',
          size: 250,
          children: [
            { name: 'ColorPalette', size: 6367 },
            { name: 'Palette', size: 1229 },
            { name: 'ShapePalette', size: 2059 },
            { name: 'SizePalette', size: 2291 }
          ]
        },
        { name: 'Property', size: 5559 },
        { name: 'Shapes', size: 19118 },
        { name: 'Sort', size: 6887 },
        { name: 'Stats', size: 6557 },
        { name: 'Strings', size: 22026 }
      ]
    },
    {
      name: 'vis',
      size: 250,
      children: [
        {
          name: 'axis',
          size: 250,
          children: [
            { name: 'Axes', size: 1302 },
            { name: 'Axis', size: 24593 },
            { name: 'AxisGridLine', size: 652 },
            { name: 'AxisLabel', size: 636 },
            { name: 'CartesianAxes', size: 6703 }
          ]
        },
        {
          name: 'controls',
          size: 250,
          children: [
            { name: 'AnchorControl', size: 2138 },
            { name: 'ClickControl', size: 3824 },
            { name: 'Control', size: 1353 },
            { name: 'ControlList', size: 4665 },
            { name: 'DragControl', size: 2649 },
            { name: 'ExpandControl', size: 2832 },
            { name: 'HoverControl', size: 4896 },
            { name: 'IControl', size: 763 },
            { name: 'PanZoomControl', size: 5222 },
            { name: 'SelectionControl', size: 7862 },
            { name: 'TooltipControl', size: 8435 }
          ]
        },
        {
          name: 'data',
          size: 250,
          children: [
            { name: 'Data', size: 20544 },
            { name: 'DataList', size: 19788 },
            { name: 'DataSprite', size: 10349 },
            { name: 'EdgeSprite', size: 3301 },
            { name: 'NodeSprite', size: 19382 },
            {
              name: 'render',
              size: 250,
              children: [
                { name: 'ArrowType', size: 698 },
                { name: 'EdgeRenderer', size: 5569 },
                { name: 'IRenderer', size: 353 },
                { name: 'ShapeRenderer', size: 2247 }
              ]
            },
            { name: 'ScaleBinding', size: 11275 },
            { name: 'Tree', size: 7147 },
            { name: 'TreeBuilder', size: 9930 }
          ]
        },
        {
          name: 'events',
          size: 250,
          children: [
            { name: 'DataEvent', size: 2313 },
            { name: 'SelectionEvent', size: 1880 },
            { name: 'TooltipEvent', size: 1701 },
            { name: 'VisualizationEvent', size: 1117 }
          ]
        },
        {
          name: 'legend',
          size: 250,
          children: [
            { name: 'Legend', size: 20859 },
            { name: 'LegendItem', size: 4614 },
            { name: 'LegendRange', size: 10530 }
          ]
        },
        {
          name: 'operator',
          size: 250,
          children: [
            {
              name: 'distortion',
              size: 250,
              children: [
                { name: 'BifocalDistortion', size: 4461 },
                { name: 'Distortion', size: 6314 },
                { name: 'FisheyeDistortion', size: 3444 }
              ]
            },
            {
              name: 'encoder',
              size: 250,
              children: [
                { name: 'ColorEncoder', size: 3179 },
                { name: 'Encoder', size: 4060 },
                { name: 'PropertyEncoder', size: 4138 },
                { name: 'ShapeEncoder', size: 1690 },
                { name: 'SizeEncoder', size: 1830 }
              ]
            },
            {
              name: 'filter',
              size: 250,
              children: [
                { name: 'FisheyeTreeFilter', size: 5219 },
                { name: 'GraphDistanceFilter', size: 3165 },
                { name: 'VisibilityFilter', size: 3509 }
              ]
            },
            { name: 'IOperator', size: 1286 },
            {
              name: 'label',
              size: 250,
              children: [
                { name: 'Labeler', size: 9956 },
                { name: 'RadialLabeler', size: 3899 },
                { name: 'StackedAreaLabeler', size: 3202 }
              ]
            },
            {
              name: 'layout',
              size: 250,
              children: [
                { name: 'AxisLayout', size: 6725 },
                { name: 'BundledEdgeRouter', size: 3727 },
                { name: 'CircleLayout', size: 9317 },
                { name: 'CirclePackingLayout', size: 12003 },
                { name: 'DendrogramLayout', size: 4853 },
                { name: 'ForceDirectedLayout', size: 8411 },
                { name: 'IcicleTreeLayout', size: 4864 },
                { name: 'IndentedTreeLayout', size: 3174 },
                { name: 'Layout', size: 7881 },
                { name: 'NodeLinkTreeLayout', size: 12870 },
                { name: 'PieLayout', size: 2728 },
                { name: 'RadialTreeLayout', size: 12348 },
                { name: 'RandomLayout', size: 870 },
                { name: 'StackedAreaLayout', size: 9121 },
                { name: 'TreeMapLayout', size: 9191 }
              ]
            },
            { name: 'Operator', size: 2490 },
            { name: 'OperatorList', size: 5248 },
            { name: 'OperatorSequence', size: 4190 },
            { name: 'OperatorSwitch', size: 2581 },
            { name: 'SortOperator', size: 2023 }
          ]
        },
        { name: 'Visualization', size: 16540 }
      ]
    }
  ]
}]

/* eslint-disable */
export const mockModel = {
  "uuid": "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
  "last_modified": 1537497663000,
  "version": "3.0.0.0",
  "name": "nmodel_basic",
  "alias": "nmodel_basic",
  "owner": "ADMIN",
  "is_draft": false,
  "description": null,
  "fact_table": "DEFAULT.TEST_KYLIN_FACT",
  "management_type": "TABLE_ORIENTED",
  "lookups": [{
      "table": "DEFAULT.TEST_ORDER",
      "kind": "LOOKUP",
      "alias": "TEST_ORDER",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_ORDER.ORDER_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.ORDER_ID"
        ]
      }
    },
    {
      "table": "EDW.TEST_SELLER_TYPE_DIM",
      "kind": "LOOKUP",
      "alias": "TEST_SELLER_TYPE_DIM",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.SLR_SEGMENT_CD"
        ]
      }
    },
    {
      "table": "EDW.TEST_CAL_DT",
      "kind": "LOOKUP",
      "alias": "TEST_CAL_DT",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_CAL_DT.CAL_DT"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.CAL_DT"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_CATEGORY_GROUPINGS",
      "kind": "LOOKUP",
      "alias": "TEST_CATEGORY_GROUPINGS",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID",
          "TEST_CATEGORY_GROUPINGS.SITE_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.LEAF_CATEG_ID",
          "TEST_KYLIN_FACT.LSTG_SITE_ID"
        ]
      }
    },
    {
      "table": "EDW.TEST_SITES",
      "kind": "LOOKUP",
      "alias": "TEST_SITES",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "TEST_SITES.SITE_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.LSTG_SITE_ID"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "kind": "LOOKUP",
      "alias": "SELLER_ACCOUNT",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "SELLER_ACCOUNT.ACCOUNT_ID"
        ],
        "foreign_key": [
          "TEST_KYLIN_FACT.SELLER_ID"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "kind": "LOOKUP",
      "alias": "BUYER_ACCOUNT",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "BUYER_ACCOUNT.ACCOUNT_ID"
        ],
        "foreign_key": [
          "TEST_ORDER.BUYER_ID"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "kind": "LOOKUP",
      "alias": "SELLER_COUNTRY",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "SELLER_COUNTRY.COUNTRY"
        ],
        "foreign_key": [
          "SELLER_ACCOUNT.ACCOUNT_COUNTRY"
        ]
      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "kind": "LOOKUP",
      "alias": "BUYER_COUNTRY",
      "join": {
        "type": "LEFT",
        "primary_key": [
          "BUYER_COUNTRY.COUNTRY"
        ],
        "foreign_key": [
          "BUYER_ACCOUNT.ACCOUNT_COUNTRY"
        ]
      }
    }
  ],
  "dimensions": [{
      "table": "TEST_KYLIN_FACT",
      "columns": [
        "ORDER_ID",
        "SLR_SEGMENT_CD",
        "CAL_DT",
        "LEAF_CATEG_ID",
        "LSTG_SITE_ID",
        "SELLER_ID"
      ]
    },
    {
      "table": "TEST_ORDER",
      "columns": [
        "ORDER_ID",
        "BUYER_ID"
      ]
    },
    {
      "table": "TEST_SELLER_TYPE_DIM",
      "columns": [
        "SELLER_TYPE_CD"
      ]
    },
    {
      "table": "TEST_CAL_DT",
      "columns": [
        "CAL_DT"
      ]
    },
    {
      "table": "TEST_CATEGORY_GROUPINGS",
      "columns": [
        "LEAF_CATEG_ID",
        "SITE_ID"
      ]
    },
    {
      "table": "TEST_SITES",
      "columns": [
        "SITE_ID"
      ]
    },
    {
      "table": "SELLER_ACCOUNT",
      "columns": [
        "ACCOUNT_ID",
        "ACCOUNT_COUNTRY"
      ]
    },
    {
      "table": "BUYER_ACCOUNT",
      "columns": [
        "ACCOUNT_ID",
        "ACCOUNT_COUNTRY"
      ]
    },
    {
      "table": "SELLER_COUNTRY",
      "columns": [
        "COUNTRY"
      ]
    },
    {
      "table": "BUYER_COUNTRY",
      "columns": [
        "COUNTRY"
      ]
    }
  ],
  "metrics": [],
  "filter_condition": null,
  "partition_desc": {
    "partition_date_column": "TEST_KYLIN_FACT.CAL_DT",
    "partition_time_column": null,
    "partition_date_start": 0,
    "partition_date_format": "yyyy-MM-dd",
    "partition_time_format": "HH:mm:ss",
    "partition_type": "APPEND",
    "partition_condition_builder": "org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder"
  },
  "capacity": "MEDIUM",
  "auto_merge": true,
  "auto_merge_time_ranges": [
    "WEEK",
    "MONTH"
  ],
  "volatile_range": {
    "volatileRangeType": "DAY",
    "volatile_range_number": 0,
    "volatile_range_available": true,
    "volatile_range_type": "DAY"
  },
  "table_positions": [{
    "table1": {
      "x_position": 2,
      "y_position": 2,
      "width": 2,
      "height": 2
    }
  }],
  "scale": 0,
  "all_named_columns": [{
      "id": 0,
      "name": "SITE_NAME",
      "column": "TEST_SITES.SITE_NAME",
      "is_dimension": false
    },
    {
      "id": 1,
      "name": "TRANS_ID",
      "column": "TEST_KYLIN_FACT.TRANS_ID",
      "is_dimension": false
    },
    {
      "id": 2,
      "name": "CAL_DT",
      "column": "TEST_KYLIN_FACT.CAL_DT",
      "is_dimension": true
    },
    {
      "id": 3,
      "name": "LSTG_FORMAT_NAME",
      "column": "TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
      "is_dimension": false
    },
    {
      "id": 4,
      "name": "LSTG_SITE_ID",
      "column": "TEST_KYLIN_FACT.LSTG_SITE_ID",
      "is_dimension": true
    },
    {
      "id": 5,
      "name": "META_CATEG_NAME",
      "column": "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME",
      "is_dimension": false
    },
    {
      "id": 6,
      "name": "CATEG_LVL2_NAME",
      "column": "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME",
      "is_dimension": false
    },
    {
      "id": 7,
      "name": "CATEG_LVL3_NAME",
      "column": "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME",
      "is_dimension": false
    },
    {
      "id": 8,
      "name": "LEAF_CATEG_ID",
      "column": "TEST_KYLIN_FACT.LEAF_CATEG_ID",
      "is_dimension": true
    },
    {
      "id": 9,
      "name": "SELLER_ID",
      "column": "TEST_KYLIN_FACT.SELLER_ID",
      "is_dimension": true
    },
    {
      "id": 10,
      "name": "WEEK_BEG_DT",
      "column": "TEST_CAL_DT.WEEK_BEG_DT",
      "tomb": true,
      "is_dimension": false
    },
    {
      "id": 11,
      "name": "PRICE",
      "column": "TEST_KYLIN_FACT.PRICE",
      "is_dimension": false
    },
    {
      "id": 12,
      "name": "ITEM_COUNT",
      "column": "TEST_KYLIN_FACT.ITEM_COUNT",
      "is_dimension": false
    },
    {
      "id": 13,
      "name": "ORDER_ID",
      "column": "TEST_KYLIN_FACT.ORDER_ID",
      "is_dimension": true
    },
    {
      "id": 14,
      "name": "TEST_DATE_ENC",
      "column": "TEST_ORDER.TEST_DATE_ENC",
      "is_dimension": false
    },
    {
      "id": 15,
      "name": "TEST_TIME_ENC",
      "column": "TEST_ORDER.TEST_TIME_ENC",
      "is_dimension": false
    },
    {
      "id": 16,
      "name": "SLR_SEGMENT_CD",
      "column": "TEST_KYLIN_FACT.SLR_SEGMENT_CD",
      "is_dimension": true
    },
    {
      "id": 17,
      "name": "BUYER_ID",
      "column": "TEST_ORDER.BUYER_ID",
      "is_dimension": true
    },
    {
      "id": 18,
      "name": "SELLER_BUYER_LEVEL",
      "column": "SELLER_ACCOUNT.ACCOUNT_BUYER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 19,
      "name": "SELLER_SELLER_LEVEL",
      "column": "SELLER_ACCOUNT.ACCOUNT_SELLER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 20,
      "name": "SELLER_COUNTRY",
      "column": "SELLER_ACCOUNT.ACCOUNT_COUNTRY",
      "is_dimension": false
    },
    {
      "id": 21,
      "name": "SELLER_COUNTRY_NAME",
      "column": "SELLER_COUNTRY.NAME",
      "is_dimension": false
    },
    {
      "id": 22,
      "name": "BUYER_BUYER_LEVEL",
      "column": "BUYER_ACCOUNT.ACCOUNT_BUYER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 23,
      "name": "BUYER_SELLER_LEVEL",
      "column": "BUYER_ACCOUNT.ACCOUNT_SELLER_LEVEL",
      "is_dimension": false
    },
    {
      "id": 24,
      "name": "BUYER_COUNTRY",
      "column": "BUYER_ACCOUNT.ACCOUNT_COUNTRY",
      "is_dimension": false
    },
    {
      "id": 25,
      "name": "BUYER_COUNTRY_NAME",
      "column": "BUYER_COUNTRY.NAME",
      "is_dimension": false
    },
    {
      "id": 26,
      "name": "TEST_COUNT_DISTINCT_BITMAP",
      "column": "TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP",
      "is_dimension": false
    }
  ],
  "all_measures": [{
      "name": "TRANS_CNT",
      "function": {
        "expression": "COUNT",
        "parameter": {
          "type": "constant",
          "value": "1"
        },
        "returntype": "bigint"
      },
      "id": 1000
    },
    {
      "name": "GMV_SUM",
      "function": {
        "expression": "SUM",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "decimal(19,4)"
      },
      "id": 1001
    },
    {
      "name": "GMV_MIN",
      "function": {
        "expression": "MIN",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "decimal(19,4)"
      },
      "id": 1002
    },
    {
      "name": "GMV_MAX",
      "function": {
        "expression": "MAX",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "decimal(19,4)"
      },
      "id": 1003
    },
    {
      "name": "ITEM_COUNT_SUM",
      "function": {
        "expression": "SUM",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.ITEM_COUNT"
        },
        "returntype": "bigint"
      },
      "id": 1004
    },
    {
      "name": "ITEM_COUNT_MAX",
      "function": {
        "expression": "MAX",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.ITEM_COUNT"
        },
        "returntype": "bigint"
      },
      "id": 1005
    },
    {
      "name": "ITEM_COUNT_MIN",
      "function": {
        "expression": "MIN",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.ITEM_COUNT"
        },
        "returntype": "bigint"
      },
      "id": 1006,
      "tomb": true
    },
    {
      "name": "SELLER_HLL",
      "function": {
        "expression": "COUNT_DISTINCT",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.SELLER_ID"
        },
        "returntype": "hllc(10)"
      },
      "id": 1007
    },
    {
      "name": "COUNT_DISTINCT",
      "function": {
        "expression": "COUNT_DISTINCT",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.LSTG_FORMAT_NAME"
        },
        "returntype": "hllc(10)"
      },
      "id": 1008
    },
    {
      "name": "TOP_SELLER",
      "function": {
        "expression": "TOP_N",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE",
          "next_parameter": {
            "type": "column",
            "value": "TEST_KYLIN_FACT.SELLER_ID"
          }
        },
        "returntype": "topn(100, 4)",
        "configuration": {
          "topn.encoding.TEST_KYLIN_FACT.SELLER_ID": "int:4"
        }
      },
      "id": 1009
    },
    {
      "name": "TEST_COUNT_DISTINCT_BITMAP",
      "function": {
        "expression": "COUNT_DISTINCT",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP"
        },
        "returntype": "bitmap"
      },
      "id": 1010
    },
    {
      "name": "GVM_PERCENTILE",
      "function": {
        "expression": "PERCENTILE_APPROX",
        "parameter": {
          "type": "column",
          "value": "TEST_KYLIN_FACT.PRICE"
        },
        "returntype": "percentile(100)"
      },
      "id": 1011
    }
  ],
  "column_correlations": [{
      "name": "CATEGORY_HIERARCHY",
      "correlation_type": "hierarchy",
      "columns": [
        "TEST_CATEGORY_GROUPINGS.META_CATEG_NAME",
        "TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME",
        "TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME"
      ]
    },
    {
      "name": "DATE_HIERARCHY",
      "correlation_type": "hierarchy",
      "columns": [
        "TEST_CAL_DT.WEEK_BEG_DT",
        "TEST_KYLIN_FACT.CAL_DT"
      ]
    },
    {
      "name": "SITE_JOINT",
      "correlation_type": "joint",
      "columns": [
        "TEST_KYLIN_FACT.LSTG_SITE_ID",
        "TEST_SITES.SITE_NAME"
      ]
    }
  ],
  "multilevel_partition_cols": [],
  "computed_columns": [],
  "status": "READY",
  "segment_ranges": {},
  "simple_tables": [{
      "table": "DEFAULT.TEST_KYLIN_FACT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar"
      }
    },
    {
      "table": "DEFAULT.TEST_ORDER",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar"

      }
    },
    {
      "table": "EDW.TEST_SELLER_TYPE_DIM",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint"

      }
    },
    {
      "table": "EDW.TEST_CAL_DT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint",
        "DIM_CRE_USER": "varchar",
        "WEEK_BEG_DT": "date"

      }
    },
    {
      "table": "DEFAULT.TEST_CATEGORY_GROUPINGS",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint"

      }
    },
    {
      "table": "EDW.TEST_SITES",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint",
        "DIM_CRE_USER": "varchar",
        "WEEK_BEG_DT": "date",
        "CAL_DT_MNS_1QTR_DT": "varchar",
        "WEEK_IN_YEAR_ID": "varchar",
        "CATEG_LVL2_NAME": "varchar",
        "MONTH_BEG_DT": "date"

      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint",
        "DIM_CRE_USER": "varchar",
        "WEEK_BEG_DT": "date"

      }
    },
    {
      "table": "DEFAULT.TEST_ACCOUNT",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint",
        "AGE_FOR_RTL_QTR_ID": "smallint"

      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer"

      }
    },
    {
      "table": "DEFAULT.TEST_COUNTRY",
      "columns": {
        "WEEK_BEG_DATE": "varchar",
        "YEAR_OF_CAL_ID": "smallint",
        "SELLER_ID": "integer",
        "CAL_DT_UPD_DATE": "varchar",
        "CAL_DT_UPD_USER": "varchar",
        "TEST_COUNT_DISTINCT_BITMAP": "varchar",
        "SITE_CNTRY_ID": "integer",
        "BUYER_ID": "bigint"

      }
    }
  ]
}

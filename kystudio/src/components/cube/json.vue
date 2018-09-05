<template>
<div class="ksd-mt-10">
  <div id="visualization"></div>
</div>
</template>
<script>
import { CodeFlower } from 'util/code_flower'
export default {
  name: 'showjson',
  props: ['json', 'index'],
  data () {
    return {
      cubePlan: {
        'uuid': '62b3c058-5514-436b-b6b5-6240a8d900af',
        'last_modified': 1509531215483,
        'version': '2.3.0.20500',
        name: 'ncube_basic',
        'model_name': 'nmodel_basic',
        'description': 'test_description',
        'dimensions': [
          {
            'id': 0,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 1,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 2,
            'encoding': {
              name: 'date',
              'version': '1'
            }
          },
          {
            'id': 3,
            'encoding': {
              name: 'fixed_length:10',
              'version': '1'
            }
          },
          {
            'id': 4,
            'encoding': {
              name: 'fixed_length:10',
              'version': '1'
            }
          },
          {
            'id': 5,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 6,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 7,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 8,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 9,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 11,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 12,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 13,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 14,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 15,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 16,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 17,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 18,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 19,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 20,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 21,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 22,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 23,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 24,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 25,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          },
          {
            'id': 26,
            'encoding': {
              name: 'dict',
              'version': '1'
            }
          }
        ],
        'cuboids': [
          {
            'id': 1000000,
            'dimensions': [
              0,
              1,
              2,
              3,
              4,
              5,
              6,
              7,
              8,
              9,
              13,
              14,
              15,
              16,
              17,
              18,
              19,
              20,
              21,
              22,
              23,
              24,
              25
            ],
            'measures': [
              1000,
              1001,
              1002,
              1003,
              1004,
              1005,
              1007,
              1008,
              1009,
              1010,
              1011
            ],
            'layouts': [
              {
                'id': 1000001,
                'rowkeys': [
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 3,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 4,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 5,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 6,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 7,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 8,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 9,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 13,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 14,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 15,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 16,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 17,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 18,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 19,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 20,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 21,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 22,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 23,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 24,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 25,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2,
                      3,
                      4,
                      5,
                      6,
                      7,
                      8,
                      9,
                      13,
                      14,
                      15,
                      16,
                      17,
                      18,
                      19,
                      20,
                      21,
                      22,
                      23,
                      24,
                      25
                    ]
                  }
                ],
                'measure_cf': [
                  {
                    name: 'M1',
                    'columns': [
                      1000,
                      1001,
                      1002,
                      1003,
                      1004,
                      1005,
                      1007,
                      1008,
                      1009,
                      1010,
                      1011
                    ]
                  }
                ],
                'storage_type': 20
              }
            ]
          },
          {
            'id': 0,
            'dimensions': [
              0,
              1
            ],
            'measures': [
              1000,
              1003
            ],
            'layouts': [
              {
                'id': 1,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1
                    ]
                  }
                ],
                'measure_cf': [
                  {
                    name: 'M1',
                    'columns': [
                      1000,
                      1003
                    ]
                  }
                ],
                'storage_type': 20
              },
              {
                'id': 2,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1
                    ]
                  }
                ],
                'measure_cf': [
                  {
                    name: 'M1',
                    'columns': [
                      1000
                    ]
                  },
                  {
                    name: 'M2',
                    'columns': [
                      1003
                    ]
                  }
                ],
                'storage_type': 20
              }
            ]
          },
          {
            'id': 1000,
            'dimensions': [
              0,
              1,
              2
            ],
            'measures': [
              1000
            ],
            'layouts': [
              {
                'id': 1001,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2
                    ]
                  }
                ],
                'measure_cf': [
                  {
                    name: 'M1',
                    'columns': [
                      1000
                    ]
                  }
                ],
                'storage_type': 20
              },
              {
                'id': 1002,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2
                    ]
                  }
                ],
                'measure_cf': [
                  {
                    name: 'M1',
                    'columns': [
                      1000
                    ]
                  }
                ],
                'storage_type': 20
              }
            ]
          },
          {
            'id': 2000,
            'dimensions': [
              0,
              1,
              2
            ],
            'measures': [
              1000,
              1003
            ],
            'layouts': [
              {
                'id': 2001,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2
                    ]
                  }
                ],
                'measure_cf': [
                  {
                    name: 'M1',
                    'columns': [
                      1000,
                      1003
                    ]
                  }
                ],
                'storage_type': 20
              }
            ]
          },
          {
            'id': 3000,
            'dimensions': [
              0,
              1,
              2,
              4
            ],
            'measures': [
              1000,
              1003,
              1005
            ],
            'layouts': [
              {
                'id': 3001,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 4,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2,
                      4
                    ]
                  }
                ],
                'measure_cf': [
                  {
                    name: 'M1',
                    'columns': [
                      1000,
                      1003,
                      1005
                    ]
                  }
                ],
                'storage_type': 20
              }
            ]
          },
          {
            'id': 1000000000,
            'dimensions': [
              0,
              1,
              2,
              4,
              11
            ],
            'layouts': [
              {
                'id': 1000000001,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 4,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 11,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2,
                      4,
                      11
                    ]
                  }
                ],
                'sort_by_columns': [
                  0
                ],
                'storage_type': 20
              }
            ]
          },
          {
            'id': 1000001000,
            'dimensions': [
              0,
              1,
              2
            ],
            'layouts': [
              {
                'id': 1000001001,
                'rowkeys': [
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2
                    ]
                  }
                ],
                'sort_by_columns': [
                  0,
                  2
                ],
                'storage_type': 20
              }
            ]
          },
          {
            'id': 1000002000,
            'dimensions': [
              0,
              1,
              2,
              3,
              4,
              5,
              6,
              7,
              8,
              9,
              11,
              12,
              13,
              14,
              15,
              16,
              17,
              18,
              19,
              20,
              21,
              22,
              23,
              24,
              25,
              26
            ],
            'layouts': [
              {
                'id': 1000002001,
                'rowkeys': [
                  {
                    'dimension_id': 0,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 1,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 2,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 3,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 4,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 5,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 6,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 7,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 8,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 9,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 11,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 12,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 13,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 14,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 15,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 16,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 17,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 18,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 19,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 20,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 21,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 22,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 23,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 24,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 25,
                    'index': 'eq'
                  },
                  {
                    'dimension_id': 26,
                    'index': 'eq'
                  }
                ],
                'dim_cf': [
                  {
                    name: 'D1',
                    'columns': [
                      0,
                      1,
                      2,
                      3,
                      4,
                      5,
                      6,
                      7,
                      8,
                      9,
                      11,
                      12,
                      13,
                      14,
                      15,
                      16,
                      17,
                      18,
                      19,
                      20,
                      21,
                      22,
                      23,
                      24,
                      25,
                      26
                    ]
                  }
                ],
                'sort_by_columns': [
                  0
                ],
                'storage_type': 20
              }
            ]
          }
        ],
        'dictionaries': [
          {
            'id': 26,
            'builder': 'org.apache.kylin.dict.NGlobalDictionaryBuilder2'
          }
        ],
        'override_properties': {},
        'segment_range_start': 0,
        'segment_range_end': 9223372036854775807,
        'auto_merge_time_ranges': null,
        'retention_range': 0,
        'notify_list': null,
        'status_need_notify': [],
        'engine_type': 0
      },
      treeMeta: {
        'id': 'Myriel',
        'group': 1,
        children: [{
          'id': 'Napoleon',
          'group': 2,
          children: [
            {'id': 'Geborand', 'group': 2},
            {'id': 'Champtercier', 'group': 2},
            {'id': 'Cravatte', 'group': 2},
            {'id': 'Count', 'group': 2}
          ]}, {
            'id': 'Mlle.Baptistine',
            'group': 3,
            children: [
              {'id': 'OldMan', 'group': 3},
              {'id': 'Labarre', 'group': 3},
              {'id': 'Valjean', 'group': 3},
              {'id': 'Marguerite', 'group': 3}
            ]}, {
              'id': 'Mme.Magloire',
              'group': 4,
              children: [
                {'id': 'Mme.deR', 'group': 4},
                {'id': 'Tholomyes', 'group': 4},
                {'id': 'Isabeau', 'group': 4},
                {'id': 'Gervais', 'group': 4}
              ]}, {
                'id': 'CountessdeLo',
                'group': 5,
                children: [
                  {'id': 'Listolier', 'group': 5},
                  {'id': 'Fameuil', 'group': 5},
                  {'id': 'Blacheville', 'group': 5},
                  {'id': 'Favourite', 'group': 5}
                ]}
        ]
      },
      /* eslint-disable */
      flowerJson: {
         "name": "flare",
         "size": 2025,
         "children": [
          {
           "name": "analytics",
           "size": 250,
           "children": [
            {
             "name": "cluster",
             "size": 250,
             "children": [
              {"name": "AgglomerativeCluster", "size": 3938},
              {"name": "CommunityStructure", "size": 3812},
              {"name": "HierarchicalCluster", "size": 6714},
              {"name": "MergeEdge", "size": 743}
             ]
            },
            {
             "name": "graph",
             "size": 250,
             "children": [
              {"name": "BetweennessCentrality", "size": 3534},
              {"name": "LinkDistance", "size": 5731},
              {"name": "MaxFlowMinCut", "size": 7840},
              {"name": "ShortestPaths", "size": 5914},
              {"name": "SpanningTree", "size": 3416}
             ]
            },
            {
             "name": "optimization",
             "size": 250,
             "children": [
              {"name": "AspectRatioBanker", "size": 7074}
             ]
            }
           ]
          },
          {
           "name": "animate",
           "size": 250,
           "children": [
            {"name": "Easing", "size": 17010},
            {"name": "FunctionSequence", "size": 5842},
            {
             "name": "interpolate",
             "size": 250,
             "children": [
              {"name": "ArrayInterpolator", "size": 1983},
              {"name": "ColorInterpolator", "size": 2047},
              {"name": "DateInterpolator", "size": 1375},
              {"name": "Interpolator", "size": 8746},
              {"name": "MatrixInterpolator", "size": 2202},
              {"name": "NumberInterpolator", "size": 1382},
              {"name": "ObjectInterpolator", "size": 1629},
              {"name": "PointInterpolator", "size": 1675},
              {"name": "RectangleInterpolator", "size": 2042}
             ]
            },
            {"name": "ISchedulable", "size": 1041},
            {"name": "Parallel", "size": 5176},
            {"name": "Pause", "size": 449},
            {"name": "Scheduler", "size": 5593},
            {"name": "Sequence", "size": 5534},
            {"name": "Transition", "size": 9201},
            {"name": "Transitioner", "size": 19975},
            {"name": "TransitionEvent", "size": 1116},
            {"name": "Tween", "size": 6006}
           ]
          },
          {
           "name": "data",
           "size": 250,
           "children": [
            {
             "name": "converters",
             "size": 250,
             "children": [
              {"name": "Converters", "size": 721},
              {"name": "DelimitedTextConverter", "size": 4294},
              {"name": "GraphMLConverter", "size": 9800},
              {"name": "IDataConverter", "size": 1314},
              {"name": "JSONConverter", "size": 2220}
             ]
            },
            {"name": "DataField", "size": 1759},
            {"name": "DataSchema", "size": 2165},
            {"name": "DataSet", "size": 586},
            {"name": "DataSource", "size": 3331},
            {"name": "DataTable", "size": 772},
            {"name": "DataUtil", "size": 3322}
           ]
          },
          {
           "name": "display",
           "size": 250,
           "children": [
            {"name": "DirtySprite", "size": 8833},
            {"name": "LineSprite", "size": 1732},
            {"name": "RectSprite", "size": 3623},
            {"name": "TextSprite", "size": 10066}
           ]
          },
          {
           "name": "flex",
           "size": 250,
           "children": [
            {"name": "FlareVis", "size": 4116}
           ]
          },
          {
           "name": "physics",
           "size": 250,
           "children": [
            {"name": "DragForce", "size": 1082},
            {"name": "GravityForce", "size": 1336},
            {"name": "IForce", "size": 319},
            {"name": "NBodyForce", "size": 10498},
            {"name": "Particle", "size": 2822},
            {"name": "Simulation", "size": 9983},
            {"name": "Spring", "size": 2213},
            {"name": "SpringForce", "size": 1681}
           ]
          },
          {
           "name": "query",
           "size": 250,
           "children": [
            {"name": "AggregateExpression", "size": 1616},
            {"name": "And", "size": 1027},
            {"name": "Arithmetic", "size": 3891},
            {"name": "Average", "size": 891},
            {"name": "BinaryExpression", "size": 2893},
            {"name": "Comparison", "size": 5103},
            {"name": "CompositeExpression", "size": 3677},
            {"name": "Count", "size": 781},
            {"name": "DateUtil", "size": 4141},
            {"name": "Distinct", "size": 933},
            {"name": "Expression", "size": 5130},
            {"name": "ExpressionIterator", "size": 3617},
            {"name": "Fn", "size": 3240},
            {"name": "If", "size": 2732},
            {"name": "IsA", "size": 2039},
            {"name": "Literal", "size": 1214},
            {"name": "Match", "size": 3748},
            {"name": "Maximum", "size": 843},
            {
             "name": "methods",
             "size": 250,
             "children": [
              {"name": "add", "size": 593},
              {"name": "and", "size": 330},
              {"name": "average", "size": 287},
              {"name": "count", "size": 277},
              {"name": "distinct", "size": 292},
              {"name": "div", "size": 595},
              {"name": "eq", "size": 594},
              {"name": "fn", "size": 460},
              {"name": "gt", "size": 603},
              {"name": "gte", "size": 625},
              {"name": "iff", "size": 748},
              {"name": "isa", "size": 461},
              {"name": "lt", "size": 597},
              {"name": "lte", "size": 619},
              {"name": "max", "size": 283},
              {"name": "min", "size": 283},
              {"name": "mod", "size": 591},
              {"name": "mul", "size": 603},
              {"name": "neq", "size": 599},
              {"name": "not", "size": 386},
              {"name": "or", "size": 323},
              {"name": "orderby", "size": 307},
              {"name": "range", "size": 772},
              {"name": "select", "size": 296},
              {"name": "stddev", "size": 363},
              {"name": "sub", "size": 600},
              {"name": "sum", "size": 280},
              {"name": "update", "size": 307},
              {"name": "variance", "size": 335},
              {"name": "where", "size": 299},
              {"name": "xor", "size": 354},
              {"name": "_", "size": 264}
             ]
            },
            {"name": "Minimum", "size": 843},
            {"name": "Not", "size": 1554},
            {"name": "Or", "size": 970},
            {"name": "Query", "size": 13896},
            {"name": "Range", "size": 1594},
            {"name": "StringUtil", "size": 4130},
            {"name": "Sum", "size": 791},
            {"name": "Variable", "size": 1124},
            {"name": "Variance", "size": 1876},
            {"name": "Xor", "size": 1101}
           ]
          },
          {
           "name": "scale",
           "size": 250,
           "children": [
            {"name": "IScaleMap", "size": 2105},
            {"name": "LinearScale", "size": 1316},
            {"name": "LogScale", "size": 3151},
            {"name": "OrdinalScale", "size": 3770},
            {"name": "QuantileScale", "size": 2435},
            {"name": "QuantitativeScale", "size": 4839},
            {"name": "RootScale", "size": 1756},
            {"name": "Scale", "size": 4268},
            {"name": "ScaleType", "size": 1821},
            {"name": "TimeScale", "size": 5833}
           ]
          },
          {
           "name": "util",
           "size": 250,
           "children": [
            {"name": "Arrays", "size": 8258},
            {"name": "Colors", "size": 10001},
            {"name": "Dates", "size": 8217},
            {"name": "Displays", "size": 12555},
            {"name": "Filter", "size": 2324},
            {"name": "Geometry", "size": 10993},
            {
             "name": "heap",
             "size": 250,
             "children": [
              {"name": "FibonacciHeap", "size": 9354},
              {"name": "HeapNode", "size": 1233}
             ]
            },
            {"name": "IEvaluable", "size": 335},
            {"name": "IPredicate", "size": 383},
            {"name": "IValueProxy", "size": 874},
            {
             "name": "math",
             "size": 250,
             "children": [
              {"name": "DenseMatrix", "size": 3165},
              {"name": "IMatrix", "size": 2815},
              {"name": "SparseMatrix", "size": 3366}
             ]
            },
            {"name": "Maths", "size": 17705},
            {"name": "Orientation", "size": 1486},
            {
             "name": "palette",
             "size": 250,
             "children": [
              {"name": "ColorPalette", "size": 6367},
              {"name": "Palette", "size": 1229},
              {"name": "ShapePalette", "size": 2059},
              {"name": "SizePalette", "size": 2291}
             ]
            },
            {"name": "Property", "size": 5559},
            {"name": "Shapes", "size": 19118},
            {"name": "Sort", "size": 6887},
            {"name": "Stats", "size": 6557},
            {"name": "Strings", "size": 22026}
           ]
          },
          {
           "name": "vis",
           "size": 250,
           "children": [
            {
             "name": "axis",
             "size": 250,
             "children": [
              {"name": "Axes", "size": 1302},
              {"name": "Axis", "size": 24593},
              {"name": "AxisGridLine", "size": 652},
              {"name": "AxisLabel", "size": 636},
              {"name": "CartesianAxes", "size": 6703}
             ]
            },
            {
             "name": "controls",
             "size": 250,
             "children": [
              {"name": "AnchorControl", "size": 2138},
              {"name": "ClickControl", "size": 3824},
              {"name": "Control", "size": 1353},
              {"name": "ControlList", "size": 4665},
              {"name": "DragControl", "size": 2649},
              {"name": "ExpandControl", "size": 2832},
              {"name": "HoverControl", "size": 4896},
              {"name": "IControl", "size": 763},
              {"name": "PanZoomControl", "size": 5222},
              {"name": "SelectionControl", "size": 7862},
              {"name": "TooltipControl", "size": 8435}
             ]
            },
            {
             "name": "data",
             "size": 250,
             "children": [
              {"name": "Data", "size": 20544},
              {"name": "DataList", "size": 19788},
              {"name": "DataSprite", "size": 10349},
              {"name": "EdgeSprite", "size": 3301},
              {"name": "NodeSprite", "size": 19382},
              {
               "name": "render",
               "size": 250,
               "children": [
                {"name": "ArrowType", "size": 698},
                {"name": "EdgeRenderer", "size": 5569},
                {"name": "IRenderer", "size": 353},
                {"name": "ShapeRenderer", "size": 2247}
               ]
              },
              {"name": "ScaleBinding", "size": 11275},
              {"name": "Tree", "size": 7147},
              {"name": "TreeBuilder", "size": 9930}
             ]
            },
            {
             "name": "events",
             "size": 250,
             "children": [
              {"name": "DataEvent", "size": 2313},
              {"name": "SelectionEvent", "size": 1880},
              {"name": "TooltipEvent", "size": 1701},
              {"name": "VisualizationEvent", "size": 1117}
             ]
            },
            {
             "name": "legend",
             "size": 250,
             "children": [
              {"name": "Legend", "size": 20859},
              {"name": "LegendItem", "size": 4614},
              {"name": "LegendRange", "size": 10530}
             ]
            },
            {
             "name": "operator",
             "size": 250,
             "children": [
              {
               "name": "distortion",
               "size": 250,
               "children": [
                {"name": "BifocalDistortion", "size": 4461},
                {"name": "Distortion", "size": 6314},
                {"name": "FisheyeDistortion", "size": 3444}
               ]
              },
              {
               "name": "encoder",
               "size": 250,
               "children": [
                {"name": "ColorEncoder", "size": 3179},
                {"name": "Encoder", "size": 4060},
                {"name": "PropertyEncoder", "size": 4138},
                {"name": "ShapeEncoder", "size": 1690},
                {"name": "SizeEncoder", "size": 1830}
               ]
              },
              {
               "name": "filter",
               "size": 250,
               "children": [
                {"name": "FisheyeTreeFilter", "size": 5219},
                {"name": "GraphDistanceFilter", "size": 3165},
                {"name": "VisibilityFilter", "size": 3509}
               ]
              },
              {"name": "IOperator", "size": 1286},
              {
               "name": "label",
               "size": 250,
               "children": [
                {"name": "Labeler", "size": 9956},
                {"name": "RadialLabeler", "size": 3899},
                {"name": "StackedAreaLabeler", "size": 3202}
               ]
              },
              {
               "name": "layout",
               "size": 250,
               "children": [
                {"name": "AxisLayout", "size": 6725},
                {"name": "BundledEdgeRouter", "size": 3727},
                {"name": "CircleLayout", "size": 9317},
                {"name": "CirclePackingLayout", "size": 12003},
                {"name": "DendrogramLayout", "size": 4853},
                {"name": "ForceDirectedLayout", "size": 8411},
                {"name": "IcicleTreeLayout", "size": 4864},
                {"name": "IndentedTreeLayout", "size": 3174},
                {"name": "Layout", "size": 7881},
                {"name": "NodeLinkTreeLayout", "size": 12870},
                {"name": "PieLayout", "size": 2728},
                {"name": "RadialTreeLayout", "size": 12348},
                {"name": "RandomLayout", "size": 870},
                {"name": "StackedAreaLayout", "size": 9121},
                {"name": "TreeMapLayout", "size": 9191}
               ]
              },
              {"name": "Operator", "size": 2490},
              {"name": "OperatorList", "size": 5248},
              {"name": "OperatorSequence", "size": 4190},
              {"name": "OperatorSwitch", "size": 2581},
              {"name": "SortOperator", "size": 2023}
             ]
            },
            {"name": "Visualization", "size": 16540}
           ]
          }
         ]
        }
    }
  },
  methods: {
    createFlower () {
      const myFlower = new CodeFlower('#visualization', 700, 400)
      myFlower.update(this.flowerJson)
    }
  }
}
</script>
<style lang='less'>
  @import '../../assets/styles/variables.less';
  #visualization {
    height: 100%;
    width: 100%;

    .node {
      cursor: pointer;
      stroke: #3182bd;
      stroke-width: 1.5px;
    }

    .link {
      fill: none;
      stroke: #9ecae1;
      stroke-width: 1.5px;
    }
  }
</style>

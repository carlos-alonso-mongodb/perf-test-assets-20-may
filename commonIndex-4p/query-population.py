{
  index: "commonIndex-4p",
  returnStoredSource: true,
  // ← use your index name
  compound: {
    filter: [
      {
        embeddedDocument: {
          path: "sn",
          operator: {
            compound: {
              filter: [
                {
                  equals: {
                    path: "sn.d.T",
                    value: "A"
                  }
                },
                {
                  equals: {
                    path: "sn.d.ani",
                    value: 13
                  }
                },
                {
                  equals: {
                    path: "sn.a",
                    value: 11
                  }
                },
                {
                  equals: {
                    path: "sn.a",
                    value: 12
                  }
                },
                {
                  range: {
                    path: "sn.d.time.v",
                    gte: ISODate(
                      "2000-04-13T07:54:16.345Z"
                    ),
                    lte: ISODate(
                      "2025-04-13T07:54:16.345Z"
                    )
                  }
                },
                {
                  equals: {
                    path: "sn.d.op.pf.ids.id",
                    value: "P..A"
                  }
                }
              ]
            }
          }
        }
      },
      {
        embeddedDocument: {
          path: "sn",
          operator: {
            compound: {
              filter: [
                {
                  equals: {
                    path: "sn.d.ani",
                    value: 140000
                  }
                },
                {
                  equals: {
                    path: "sn.a",
                    value: 70000
                  }
                },
                {
                  equals: {
                    path: "sn.a",
                    value: 3
                  }
                },
                {
                  equals: {
                    path: "sn.a",
                    value: 11
                  }
                },
                {
                  equals: {
                    path: "sn.d.v.df.cs",
                    value: "E08001460"
                  }
                }
              ]
            }
          }
        }
      }
    ]
  }
}
# ProjectionParser
Python Module to parse, edit and recompile Vertica projections

## How to run ProjectionParser

- Initalize a `ProjParser` object
- Set the `raw_proj` property of your `ProjParser` object with the contents of a `CREATE PROJECTION` script
- Then run the `parse_projection` method.
- At that point, you can add columns to the sort order, or change from segmented to unsegmented, etc.
- Once you have made your changes to the projection, then
- Run the `recompile_projection` method
- The recompiled projection will be stored in the `recompiled_projection` property

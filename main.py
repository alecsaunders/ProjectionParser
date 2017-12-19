import re

class ProjectionObject():
    def __init__(self, create_projection_statement):
        self.raw_proj = create_projection_statement
        self.projection_database = None
        self.projection_schema = None
        self.projection_basename = None
        self.buddy = None
        self.create_type = None
        self.projection_col_list = None
        self.select_list = None
        self.from_database = None
        self.from_schema = None
        self.from_table = None
        self.order_by_list = None
        self.segmentation_spec = None
        self.offset = None
        self.ksafe = None
        
        self.set_projection_properties()

    def set_projection_properties(self):
        self.set_properties_from_create_line(self.raw_proj)

    def set_properties_from_create_line(self, script):
        hint_pattern = '\/\*.*\*\/'
        hint_search_result = re.search(hint_pattern, script)
        if hint_search_result:
            hint = hint_search_result.group(0)
            script = re.sub(hint_pattern, '', script)
        create_line = script.split('(')[0].strip()
        while '  ' in create_line:
            create_line = create_line.replace('  ', ' ')
        create_projection_pattern = re.compile('CREATE PROJECTION ', re.IGNORECASE)
        if_not_exists_pattern = re.compile('IF NOT EXISTS ', re.IGNORECASE)
        create_line = re.sub(create_projection_pattern, '', create_line)
        create_line = re.sub(if_not_exists_pattern, '', create_line)
        projection_name = create_line.strip()

        buddy_pattern = '\_b\d$'
        bx_search_result = re.search(buddy_pattern, projection_name, re.IGNORECASE)
        if bx_search_result:
            buddy_text = bx_search_result.group(0)
            projection_name = re.sub(buddy_pattern, '', projection_name)
            try:
                self.buddy = int(buddy_text[2:])
            except:
                pass

        if '.' in projection_name:
            proj_parts = projection_name.split('.')
            if len(proj_parts) == 2:
                self.projection_schema = proj_parts[0]
                self.projection_basename = proj_parts[1]
            if len(proj_parts) == 3:
                self.projection_database = proj_parts[0]
                self.projection_schema = proj_parts[1]
                self.projection_basename = proj_parts[2]
        else:
            self.projection_basename = projection_name


if __name__ == '__main__':
    proj_script = open('Files/design', 'r').read()
    proj_obj = ProjectionObject(proj_script)

    print(proj_obj.projection_database)
    print(proj_obj.projection_schema)
    print(proj_obj.projection_basename)

from generator.generate import Generate
from cassandra
class CreateStatement:

    def __init__(self, schema_name: str, table_name: int, struct: dict ):
        self.schema_name = schema_name,
        self.table_name = table_name,
        self.struct = struct
    
    def get_column_names(self):
        return self.struct.keys()
    
    def generate_inserts(self):
        stmt = f'''INSERT INTO 
                {self.schema_name}.{self.table_name} 
                {self.get_column_names} VALUES ('''
        for k,v in self.struct.items():
            if v=="int":
                stmt += "{},"
            else:
                stmt += "'{}',"
        else:
            stmt.rsplit(',',1)[0]
        stmt+=";"
    

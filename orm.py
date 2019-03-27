import psycopg2
from collections import defaultdict

conn = psycopg2.connect(
    database = "test",
    user="postgres",
    password="lina69",
    host="localhost",
    port="5432"
)
cor = conn.cursor()

TableList = []
AttrNames = {}
# defaultdict(list)

def connet():
    global TableList
    cor.execute("select table_name from information_schema.tables where table_schema='public';")
    TableList = [k[0] for k in cor]

    cor.execute("select table_name, column_name from information_schema.columns where table_schema='public';")

    global AttrNames

    for k, v in cor:
        if k in AttrNames.keys():
            AttrNames[k].append(v)
        else:
            alst = [v]
            AttrNames = {k:alst}
    

connet()

print('tab', TableList)
print ('attr', AttrNames)

class Field():
    
    # todo unique
    def __init__(self,f_type,required=False,default=None):
        self.f_type = f_type
        self.required = required
        self.default = default
    
    def validate(self,value):
        if value is None:
            if self.default is None and not self.required: 
                return None
            elif self.default is None and self.required:
                raise ValueError("Field is required")
            elif self.default is not None:
                value = self.default
        if not self.f_type(value):
            raise TypeError(f"Value '{value}' cannot be converted to type '{self.f_type}'")
        return self.f_type(value)
    

class IntField(Field):
    
    def __init__(self, required=False, default=None):
        super().__init__(int,required, default)

class StringField(Field):

        def __init__(self, required=False, default=None):
            super().__init__(str,required, default)

class Manage:
    def __init__(self):
        self.model_cls = None

    def __get__(self, instance, owner):
        if self.model_cls is None:
            self.model_cls = owner
        return self

    def create(self, **kwargs):
        print(self.model_cls._fields)

    def delete(self, **kwargs):
        pass
        # todo
    


class MetaModel(type): 

    def __new__(mcs, name, bases, namespace):
        if name == 'Model':
            return super().__new__(mcs, name, bases, namespace)

        meta = namespace.get('Meta') 
        if meta is None:
            raise ValueError('meta is none')
        if not hasattr(meta, 'table_name'):
            raise ValueError('table_name is empty')
        if meta.table_name == "":
            meta.table_name=namespace.get("__qualname__")

        fields = {k: v for k, v in namespace.items()
            if isinstance(v, Field)}

        namespace['_fields'] = fields
        namespace['_table_name'] = meta.table_name


        if meta.table_name not in TableList:
            print (meta.table_name, 'not in the list')
        else:
            for f in namespace['_fields'].keys():
                if f not in AttrNames[namespace['_table_name']]:
                    print(f, "not in the list of attributes")

        return super().__new__(mcs, name, bases, namespace)

class Model(metaclass=MetaModel):

    class Meta:
        table_name = ""

    objects = Manage()
    # objects = Manage(Meta.table_name)

class MyClass(Model):
    id = IntField()

    class Meta:
        table_name = ""

class courses(Model):
    id = IntField()
    lala = StringField()

    class Meta: 
        table_name =""
    

MyClass.objects.create()
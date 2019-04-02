import psycopg2

class Field():
    
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
    

class connection():
    
    def __init__(self,*_, database,user="postgres", password="postgres", host="localhost", port="5432"):

        conn = psycopg2.connect(
            database = database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = conn.cursor()
        self.table_cache = {}
        
        self.cur.execute(f"select table_name, column_name,data_type from information_schema.columns where table_schema='public';")

        for i, k, v in self.cur:
            if i not in self.table_cache.keys():
                dicl={k:v}
                self.table_cache[i]=dicl
            else:
                self.table_cache[i][k]=v

    def create_table(self,*_,table_name,attrs):
        if table_name not in self.table_cache.keys():
            st = 'CREATE TABLE '+table_name+' ('
            stlist = []
            for key, value in attrs.items():
                ss = key
                if isinstance(value,IntField):
                    typ = 'integer'
                elif isinstance(value,StringField):
                    typ = 'text'
                else: 
                    typ = value
                ss += ' '+ typ
                stlist.append(ss)
            st += ', '.join(stlist)
            st += ');'
            self.cur.execute(st)
            self.table_cache[table_name]=attrs

        else:
            raise ValueError(f"Table {table_name} is already exists")

    def delete_table(self,*_, table_name, attrs):

        if table_name in self.table_cache.keys():
            st = 'DROP TABLE '+table_name+";"
            self.cur.execute(st)
        else:
            raise ValueError(f"Table {table_name} doesn't exist")

    def add_attr(self,*_,table_name,attrs):

        if table_name in self.table_cache.keys():
            stlist = []
            st = 'ALTER TABLE '+table_name
            for key, value in attrs.items():
                if key not in self.table_cache[table_name]:
                    if isinstance(value,IntField):
                        typ = 'integer'
                    elif isinstance(value,StringField):
                        typ = 'text'
                    else: 
                        typ = value
                        ss = " ADD COLUMN " + key + " "+ typ
                        stlist.append(ss)
                    self.table_cache[table_name][key]=value
                else: 
                    raise ValueError(f"Attibute {key} already exists")
            st += ", ".join(stlist)
            st += ';'
            self.cur.execute(st)
        else:
            raise ValueError(f"Table {table_name} doesn't exist")

    def drop_attr(self,*args, table_name):
        
        if table_name in self.table_cache.keys():
            stlist = []
            st = 'ALTER TABLE '+table_name
            for key in args:
                if key in self.table_cache[table_name]:
                    ss = " DROP COLUMN IF EXISTS " + key
                    stlist.append(ss)
                    del self.table_cache[table_name][key]
                else: 
                    raise ValueError(f"Attibute {key} doesn't exist")
            st += ", ".join(stlist)
            st += ';'
            self.cur.execute(st)
        else:
            raise ValueError(f"Table {table_name} doesn't exist")

    def change_table(self,*_,table_name,attrs):

        if table_name in self.table_cache.keys():
            stlist = []
            st = 'ALTER TABLE '+table_name
            for key, value in attrs.items():
                if key in self.table_cache[table_name]:
                    ss = " ALTER COLUMN " + key + " TYPE "+ value
                    stlist.append(ss)
                    self.table_cache[table_name][key]=value
                else: 
                    raise ValueError(f"Attibute {key} doesn't exist")
            st += ", ".join(stlist)
            st += ';'
            self.cur.execute(st)
        else:
            raise ValueError(f"Table {table_name} doesn't exist")

bd = connection(
    database="test",
    password="lina69"
)

class Manage:
    def __init__(self):
        self.model_cls = None

    def __get__(self, instance, owner):
        if self.model_cls is None:
            self.model_cls = owner
        return self

    def create(self, bd, **kwargs):
    # Здесь я хотела сделать проверку, на уникальность значений, но в прошлый раз запуталась
    #
        st = 'INSERT INTO ' + self.model_cls._table_name + " "
        stkeys=[]
        stvalues=[]
        for k, v in kwargs.items():
            print(k)
            if k not in self.model_cls._fields:
                raise ValueError("No such attribute")
            else: 
                stkeys.append(k)
                if isinstance(self.model_cls._fields[k],IntField):
                    stvalues.append(v)
                else:
                    val = "'"+v+"'"
                    stvalues.append(val)
        st+="(" + ", ".join(stkeys) + ") VALUES "
        st+="(" + ", ".join(stvalues) + ");"
        bd.cur.execute(st)

    def all(self, bd):
        bd.cur.execute(f"SELECT * FROM {self.model_cls._table_name}")
        for str in bd.cur:
            print (str)
    #Должен ли он сохранять данные? 
    #выводить данные как словарь атрибут - значение? 

    def delete(self, bd, **kwargs):
        st = 'DELETE FROM '+self.model_cls._table_name+" WHERE "
        stlist = []
        for k, v in kwargs.items():
            if k not in self.model_cls._fields:
                raise ValueError(f"There is no attribute '{k}' in table {self.model_cls._table_name}")
            else:
                if isinstance(self.model_cls._fields[k],IntField):
                    ss = k + " = "+ str(v)
                else:
                    ss = k + " = '"+ str(v) + "'"
                stlist.append(ss)
        st += " and ".join(stlist) + ";"
        bd.cur.execute(st)

    def update(self, bd, checkdict={}, valuedict={}):
        st = "UPDATE " + self.model_cls._table_name + " SET "
        stlist=[]
        for k, v in valuedict.items():
            if isinstance(self.model_cls._fields[k],IntField):
                ss = k + " = "+ str(v)
            else:
                ss = k + " = '"+ str(v) + "'"
            stlist.append(ss)
        st += ', '.join(stlist) + " WHERE "
        stlist = []
        for k, v in checkdict.items():
            if isinstance(self.model_cls._fields[k],IntField):
                ss = k + " = "+ str(v)
            else:
                ss = k + " = '"+ str(v) + "'"
            stlist.append(ss)
        st += ' AND '.join(stlist) + ";"
        bd.cur.execute(st)


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

        # !!!!!!!!!!!!!!!!
        # attrs = {namespace[k] for k in fields.keys()}
        namespace['_fields'] = fields
        namespace['_table_name'] = meta.table_name

        for base in bases:
            for key, value in base.__dict__.items():
                if isinstance(value, Field):
                    namespace['_fields'][key]=value

        if meta.table_name not in bd.table_cache.keys():
            bd.create_table(table_name=meta.table_name,attrs=namespace['_fields'])

        return super().__new__(mcs, name, bases, namespace)

class Model(metaclass=MetaModel):

    class Meta:
        table_name = ""

    objects = Manage()
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    def __init__(self, *_, **kwargs):
        # self.objects.create(bd,kwargs)
        for field_name, field in self._fields.items():
            value = field.validate(kwargs.get(field_name))
            setattr(self, field_name, value)




# EXAMPLE


class MyClass(Model):

    id = IntField()
    onemore = StringField();

    class Meta:
        table_name = ""

class courses(Model):
    
    id = IntField()
    cour = StringField()

    class Meta: 
        table_name =""

class python(courses):
    teacher = StringField()

    class Meta:
        table_name=""

meow = python(id=1,cour='advanced',teacher='cool') 

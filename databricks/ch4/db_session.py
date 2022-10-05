from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

# DataBricks HOST & TOKEN
#os.environ["DATABRICKS_HOST"] = "https://xyz.azuredatabricks.net"
#os.environ["DATABRICKS_TOKEN"] = "dapi..."

os.environ["DATABRICKS_HOST"]="https://adb-3720311585959435.15.azuredatabricks.net"
os.environ["DATABRICKS_TOKEN"]="dapi866fb0277ee7a6a815481d98536e5f9e"

'''
Singleton Class for MySQL Database session
'''
class mysql_session:
    __shared_instance = None
    session = None
    Base = None
    engine = None

    @staticmethod
    def getInstance():
        """Static Access Method"""
        if mysql_session.__shared_instance == None:
            mysql_session()
        return mysql_session.__shared_instance

    def __init__(self):
        """virtual private constructor"""
        if mysql_session.__shared_instance != None:
            raise Exception("singleton class!")
        else:
            self.__init_db_session(self)
            mysql_session.__shared_instance = self

    @staticmethod
    def __init_db_session(self):
        self.engine = create_engine("mysql+mysqlconnector://cloud_infra_user:cloud_infra_pass@localhost/cloud_infra_db",
                               echo=True)

        conn = self.engine.connect()
        self.Base = declarative_base()

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def get_db_session(self):
        return self.session

    def get_db_Base(self):
        return self.Base, self.engine

    # main method


if __name__ == "__main__":
    # create object of Singleton Class
    obj1 = mysql_session()
    print(obj1)

    # pick the instance of the class
    obj2 = mysql_session.getInstance()
    print(obj2)

    print(obj1.get_db_session())
    print(obj2.get_db_session())


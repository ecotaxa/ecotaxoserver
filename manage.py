# manage.py

from flask_script import Manager
from flask_security.utils import encrypt_password
from flask_migrate import Migrate, MigrateCommand
from appli import db,user_datastore,database,app,g,ntcv
import shutil,os,datetime

manager = Manager(app)

migrate = Migrate(app, db)
manager.add_command('db', MigrateCommand)

@manager.command
def hello():
    print ("hello")

@manager.command
def createadminuser():
    """
    Create Admin User in the database admin/password
    """

    from appli.database import roles
    r=roles.query.filter_by(id=1).first()
    if r is None:
        print("Create role ",database.AdministratorLabel)
        db.session.add(roles(id=1,name=database.AdministratorLabel))
        db.session.commit()

    u=user_datastore.find_user(email='admin')
    if u is not None:
        print("drop user ",u)
        user_datastore.delete_user(u)
        db.session.commit()
    print("Create user 'admin' with password 'ecotaxa'")
    user_datastore.create_user(email='admin', password=encrypt_password('ecotaxa'),name="Application Administrator")
    user_datastore.add_role_to_user('admin','Application Administrator')
    db.session.commit()

    print("Add initial instance ")
    from appli.database import EcotaxaInst
    r = EcotaxaInst.query.filter_by(id=1).first()
    if r is None:
        print("Create 1st instance VLFR")
        db.session.add(EcotaxaInst( name="Oceanographic Laboratory of Villefranche sur Mer - LOV" #id=1,
                                   , url="http://127.0.0.1:5000"
                                   , sharedsecret="uVyDqG6L24NgpNDwkup3gXddUrjzrG6LYKAOksPOjHgqNPjZkKd2DTB2VzJVQAOI"
                                   ))
        db.session.add(EcotaxaInst( name="Test instance"  #id=2,
                                   , url="http://127.0.0.1:5000"
                                   , sharedsecret="aVyDqG6L24NgpNDwkup3gXddUrjzrG6LYKAOksPOjHgqNPjZkKd2DTB2VzJVQAOI"
                                   ))
        db.session.commit()


@manager.command
def dbdrop():
    db.drop_all()
@manager.command
def dbcreate():
    with app.app_context():  # Création d'un contexte pour utiliser les fonction GetAll,ExecSQL qui mémorisent
        g.db = None
        db.create_all()
        import flask_migrate
        flask_migrate.stamp(revision='head')


@manager.command
def ResetDBSequence(cur=None):
    with app.app_context():  # Création d'un contexte pour utiliser les fonction GetAll,ExecSQL qui mémorisent
        g.db = None
        print("Start Sequence Reset")
        if cur is None:
            cur=db.session
        cur.execute("SELECT setval('seq_taxonomy', (SELECT max(id) FROM taxonomy), true)")
        cur.execute("SELECT setval('seq_users', (SELECT max(id) FROM users), true)")
        cur.execute("SELECT setval('roles_id_seq', (SELECT max(id) FROM roles), true)")
        print("Sequence Reset Done")



@manager.command
def CreateDB():
    with app.app_context():  # Création d'un contexte pour utiliser les fonction GetAll,ExecSQL qui mémorisent
        g.db = None
        if input("This operation will create a new empty DB.\n If a database exists, it will DESTROY all existings data of the existing database.\nAre you SURE ? Confirm by Y !").lower()!="y":
            print("Import Aborted !!!")
            exit()

        print("Configuration is Database:",app.config['DB_DATABASE'])
        print("Login: ",app.config['DB_USER'],"/",app.config['DB_PASSWORD'])
        print("Host: ",app.config['DB_HOST'])
        import psycopg2

        print("Connect Database")
        # On se loggue en postgres pour dropper/creer les bases qui doit être déclaré trust dans hba_conf
        conn=psycopg2.connect(user='postgres',host=app.config['DB_HOST'])
        cur=conn.cursor()

        conn.set_session(autocommit=True)
        print("Drop the existing database")
        sql="DROP DATABASE IF EXISTS "+app.config['DB_DATABASE']
        cur.execute(sql)

        print("Create the new database")
        sql="create DATABASE "+app.config['DB_DATABASE']+" WITH ENCODING='LATIN1'  OWNER="+app.config['DB_USER']+" TEMPLATE=template0 LC_CTYPE='C' LC_COLLATE='C' CONNECTION LIMIT=-1 "
        cur.execute(sql)

        print("Create the Schema")
        dbcreate()
        print("Create Roles & Users")
        createadminuser()
        print("Creation Done")


@manager.command
def RecomputeDisplayName():
    print ("RecomputeDisplayName")
    with app.app_context():  # Création d'un contexte pour utiliser les fonction GetAll,ExecSQL qui mémorisent
        g.db = None
        import appli.services
        appli.services.ComputeDisplayName([])




if __name__ == "__main__":
    manager.run()
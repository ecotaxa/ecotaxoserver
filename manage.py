# manage.py

from flask_script import Manager
from flask_security.utils import encrypt_password
from flask_migrate import Migrate, MigrateCommand
from appli import db,user_datastore,database,app,g
import shutil,os

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
        db.session.add(EcotaxaInst(id=1, name="Oceanographic Laboratory of Villefranche sur Mer - LOV"
                                   , url="127.0.0.1:5000"
                                   , sharedsecret="uVyDqG6L24NgpNDwkup3gXddUrjzrG6LYKAOksPOjHgqNPjZkKd2DTB2VzJVQAOI"
                                   ))
        db.session.add(EcotaxaInst(id=2, name="Test isntance"
                                   , url="127.0.0.1:5000"
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
        database.ExecSQL("""create view v_taxotree as
select t.id,concat(t.name,'('||t1.name||')') nom,
case  when t1 is null then 1 when t2 is null then 2 when t3 is null then 3 when t4 is null then 4
      when t5 is null then 5 when t6 is null then 6 when t7 is null then 7 when t8 is null then 8
      when t9 is null then 9 when t10 is null then 10 when t11 is null then 11 when t12 is null then 12
      when t13 is null then 13 when t14 is null then 14 when t15 is null then 15 when t16 is null then 16
      when t17 is null then 17 when t18 is null then 18 when t19 is null then 19 end depth
,concat(t14.name||'>',t13.name||'>',t12.name||'>',t11.name||'>',t10.name||'>',t9.name||'>',t8.name||'>',t7.name||'>',
     t6.name||'>',t5.name||'>',t4.name||'>',t3.name||'>',t2.name||'>',t1.name||'>',t.name) tree
      from taxonomy t
      left join taxonomy t1 on t.parent_id=t1.id
      left join taxonomy t2 on t1.parent_id=t2.id
      left join taxonomy t3 on t2.parent_id=t3.id
      left join taxonomy t4 on t3.parent_id=t4.id
      left join taxonomy t5 on t4.parent_id=t5.id
      left join taxonomy t6 on t5.parent_id=t6.id
      left join taxonomy t7 on t6.parent_id=t7.id
      left join taxonomy t8 on t7.parent_id=t8.id
      left join taxonomy t9 on t8.parent_id=t9.id
      left join taxonomy t10 on t9.parent_id=t10.id
      left join taxonomy t11 on t10.parent_id=t11.id
      left join taxonomy t12 on t11.parent_id=t12.id
      left join taxonomy t13 on t12.parent_id=t13.id
      left join taxonomy t14 on t13.parent_id=t14.id
      left join taxonomy t15 on t14.parent_id=t15.id
      left join taxonomy t16 on t15.parent_id=t16.id
      left join taxonomy t17 on t16.parent_id=t17.id
      left join taxonomy t18 on t17.parent_id=t18.id
      left join taxonomy t19 on t18.parent_id=t19.id
""")


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
def FullDBRestore():
    """
    Will restore an exported DB as is and replace all existing data
    """
    from appli.tasks.taskimportdb import RestoreDBFull
    if input("This operation will import an exported DB and DESTROY all existings data of the existing database.\nAre you SURE ? Confirm by Y !").lower()!="y":
        print("Import Aborted !!!")
        exit()
    with app.app_context():  # Création d'un contexte pour utiliser les fonction GetAll,ExecSQL qui mémorisent
        g.db = None
        RestoreDBFull()

@manager.command
def RecomputeStats():
    """
    Recompute stats related on Taxonomy and Projects
    """
    import appli.cron
    with app.app_context():  # Création d'un contexte pour utiliser les fonction GetAll,ExecSQL qui mémorisent
        g.db = None
        appli.cron.RefreshAllProjectsStat()
        appli.cron.RefreshTaxoStat()

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



if __name__ == "__main__":
    manager.run()
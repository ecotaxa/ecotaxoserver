# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2016  Picheral, Colin, Irisson (UPMC-CNRS)
import flask_admin
from flask_admin import base
from flask_admin.contrib.sqla import ModelView
from flask_admin.form import SecureForm
from flask_admin.helpers import get_form_data
from flask_login import current_user
from flask_security.utils import hash_password
from wtforms.fields import TextField, PasswordField
from wtforms.validators import ValidationError
from appli import db, app, database
from appli.database import GetAll


class UsersView(ModelView):
    # Disable model creation
    can_create = True
    # Enable CSRF check
    form_base_class = SecureForm

    # Override displayed fields
    column_list = ('email', 'name','organisation','active', 'roles')
    form_columns = ('email', 'name','organisation', 'active', 'roles', 'password')
    column_searchable_list = ('email', 'name')
    form_overrides = {
        'email': TextField,
        'password': PasswordField
    }

    def __init__(self, session, **kwargs):
        # You can pass name and other parameters if you want to
        super(UsersView, self).__init__(database.users, session, **kwargs)

    def update_model(self, form, model):
        # Do not set password if its field is empty.
        if not form._fields['password'].data:
            del form._fields['password']
        else:
            form._fields['password'].data =hash_password(form._fields['password'].data)
        return super(UsersView, self).update_model(form, model)

    def create_model(self, form):
        form._fields['password'].data =hash_password(form._fields['password'].data)
        return super(UsersView, self).create_model(form)
    def checkpasswordequal(form, field):
        if field.data !=form._fields['password_confirm'].data:
            raise ValidationError("Password Confirmation doesn't match")

    def edit_form(self,obj=None):
        form = self._edit_form_class(get_form_data(), obj=obj)
        return form

    def create_form(self,obj=None):
        return self.edit_form(obj)

    def scaffold_form(self):
        form_class = super(UsersView, self).scaffold_form()
        form_class.password_confirm = PasswordField('Password Confirmation')
        return form_class
    form_args = dict(
        password=dict( validators=[checkpasswordequal])
    )
    def is_accessible(self):
        return current_user.has_role(database.AdministratorLabel)
    edit_template = 'admin/users_edit.html'
    create_template = 'admin/users_create.html'


class TaxonomyView(ModelView):
    column_list = ('id','parent_id', 'name','aphia_id')
    column_filters = ('id','parent_id', 'name','aphia_id')
    column_searchable_list = ('name',)
    page_size = 100
    def __init__(self, session, **kwargs):
        super(TaxonomyView, self).__init__(database.Taxonomy, session, **kwargs)
    def is_accessible(self):
        return current_user.has_role(database.AdministratorLabel)

# Create admin
adminApp = flask_admin.Admin(app, name='EcotaxoServer Administration',url='/admin')

# Add views
adminApp.add_view(UsersView(db.session,name="Users"))

adminApp.add_view(TaxonomyView(db.session,category='Taxonomy',name="Edit Taxonomy"))

adminApp.add_link(base.MenuLink('Import Taxonomy (admin only)', category='Taxonomy', url='/Task/Create/TaskTaxoImport'))
adminApp.add_link(base.MenuLink('Taxonomy errors (admin only)', category='Taxonomy', url='/dbadmin/viewtaxoerror'))
adminApp.add_link(base.MenuLink('EcoTaxoServer Home', url='/'))
adminApp.add_link(base.MenuLink('View DB Size (admin only)', category='Database', url='/dbadmin/viewsizes'))
adminApp.add_link(base.MenuLink('View DB Bloat (admin only)', category='Database', url='/dbadmin/viewbloat'))

adminApp.add_link(base.MenuLink('SQL Console (admin only)', category='Database', url='/dbadmin/console'))
adminApp.add_link(base.MenuLink('Recompute Projects and Taxo stat (can be long)(admin only)', category='Database', url='/dbadmin/recomputestat'))

def GetAdminList():
    LstUsers=GetAll("""select name||'('||email||')' nom from users u
                        join users_roles r on u.id=r.user_id where r.role_id=1""")
    return ", ".join([r[0] for r in LstUsers])
app.jinja_env.globals.update(GetAdminList=GetAdminList)


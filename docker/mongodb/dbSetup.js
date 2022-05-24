var user = db.getSiblingDB(_getEnv('MONGO_ADMIN_DB')).getUser(_getEnv('MONGO_ROOT_USER'));
if(user != null)
  quit(0);

var db = db.getSiblingDB(_getEnv('MONGO_DB_NAME'));
db.createUser({
  user: _getEnv('MONGO_USER'),
  pwd: _getEnv('MONGO_PWD'),
  roles: ['readWrite', 'userAdmin']
});

var col1 = db.createCollection('fescion');
var col2 = db.createCollection('stream');

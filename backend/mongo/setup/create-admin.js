db.createUser(
{
    user: _getEnv('MONGO_ADMIN_USER'),
    pwd: _getEnv('MONGO_ADMIN_PASSWORD'),
    roles: [
      { role: "root", db: "admin" }
    ]
});

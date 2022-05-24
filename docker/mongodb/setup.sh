echo ************************************
echo 'Configure ACLs and dataBase'
echo ************************************
mongo --host rs0/mongo1:27017 admin /usr/local/bin/dbSetup.js

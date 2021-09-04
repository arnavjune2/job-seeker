from flask import Flask, request, jsonify, render_template
from cassandraOperations import CassandraManagement
from cassandra.query import tuple_factory
import jinja2

jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader('templates'))


app = Flask(__name__)

clientID = 'vwxPiwfMhuaklXJwepZfUlko'
clientSec = "rOC1W+m23shzNT.hUtvjoAsDChK_arr2UKMuUbOtsQLsSXwZ0bdOUm4qJw17mLw2ZifQKfOvZhXF,AQ6.6RmpuJlHAIyfGTgaB.5iXnPkDpbmRUmhTK7zXoUvke3n_7t"
keyspace = "job_seeker"
secure_connect = "secure-connect-test1.zip"
db = CassandraManagement(clientID=clientID, clientSec=clientSec, keyspace=keyspace, secure_connect=secure_connect)
session = db.getCassandraClientObject()
session.execute('use job_seeker ;')


@app.route('/')
@app.route('/home', methods=['POST', 'GET'])
def home():
    return render_template('home.html')


@app.route('/signup', methods=['POST', 'GET'])
def sign_up():
    info = " "
    if request.method == 'POST':
        email = request.form['email'].replace('"', '\\"')
        password = request.form['password'].replace('"', '\\"')
        session = db.getCassandraClientObject()
        session.row_factory = tuple_factory
        emails = []
        cass = session.execute('SELECT email FROM login')
        for i in cass:
            emails.append(i[0])
        if email not in emails:
            print(email)
            info = "invalid email id or password"
            db.closeCassandraSession()
            return render_template('sign_up.html', info=info)
        else:
            temp = session.execute(f"SELECT password , status FROM login WHERE email = '{email}' ALLOW FILTERING ;")
            print(temp)
            if password == temp[0][0]:
                if temp[0][1] == 'employer':
                    return render_template('employer.html')
                else :
                    return render_template('employer.html')


    return render_template('sign_up.html')


@app.route('/employee')
def employee():
    session = db.getCassandraClientObject()
    table = session.execute("select company ,job_desc,skills from master")

    return render_template('employee.html', table=table)


@app.route('/employer', methods=['POST', 'GET'])
def employer():
    return render_template('employer.html')


def some_view():
    if request.method == 'POST':
        form_name = request.form['form-name']
        if form_name == 'form1':
            pass


if __name__ == "__main__":
    app.run(debug=True)

### Prerequisite 
- Copy the source code to each server.
- Each server should have Python3 installed.
- Each server should have Python3 **venv** package installed.
### Setup CockroachDB Cluster
Run `shell_scripts/install_db.sh` script on one server to install and setup a CockroachDB cluster with replica factor equal to 3. 
It requires 2 arguements: `http port number` and `db port number`.
```sh
shell_scripts/install_db.sh 8082 26278
```
- It downloads CockroachDB v21.1.7 binary to each server.
- It generates and copy node certificates to each server.
- It generates client certificate for root user.
- It starts CockraochDB process on each server.
- It initials the cluster on the server where the script runs.
### Import Data into Databases
Run `shell_scripts/db_init.sh` script to create database and import data into the database.
It requires 1 arguement: `db port number`
```sh
shell_scripts/db_init.sh 26278
```
Change root default password to `cs4224hadmin`
```sh
/temp/cs4224h/cockroach-v21.1.7.linux-amd64/cockroach sql --certs-dir=/temp/cs4224h/certs --host=xcnd45:26278 --execute='ALTER USER root WITH PASSWORD 'cs4224hadmin''
```
- It downloads and unarchive `project_files_4.zip` if not exists.
- It prepares data files by splitting `customer.csv`, `order.csv`, `order-line.csv`, and `stock.csv` equally.
- It copy the splitted data files to all other servers.
- It starts Python http.server locally on each server.
- It runs SQL statements to create database and import data into the database tables. 

### Run Performance Test on Servers
`shell_scripts/client_exec.sh` script starts client application processes against workload transactions files on a server.
It requires 1 arguement: `workload type`.
Run the script on each server of the cluster.
```sh
shell_scripts/client_exec.sh A
```
- It creates a Python virtual environment if not exists.
- It activates the virtual environment. 
- It upgrade pip version and install required packages in `requirements.txt`.
- It starts the application process, which writes stdout to `x_output.txt` and stderr to `x_performance.txt`.
### Retrieve Performance Test Results
Run `shell_scripts/get_results.sh` script to retrieve client results from all servers.
It requires 1 argument: `output directory path`
```sh
shell_scripts/get_results.sh /temp/cs4224h/cockroach_output
```
It read all performance result file `*_performance.txt` from each server and appends to `clients.csv`.
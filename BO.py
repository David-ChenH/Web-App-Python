import pymysql
import json

cnx = pymysql.connect(host='localhost',
                              user='dbuser',
                              password='dbuser',
                              db='lahman2017raw',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result


def template_to_where(t):
    sql = ""
    for (k, v) in t.items():
        if sql != "":
            sql = sql + " " + "AND" + " "
        sql = sql + k + "='" + v + "'"
    if sql != "":
        sql = "WHERE " + sql
    return sql


def find_by_primary_key(table, primary_key_values, fields=None, limit=None, offset=None):
    # sql_find_primary = "SHOW KEYS FROM " + table + " WHERE Key_name = 'PRIMARY'"
    # key_name = run_q(sql_find_primary, None, True)
    # key_columns = [k["Column_name"] for k in key_name]
    # print(key_columns)
    # t = dict(zip(key_columns, [primary_key_values]))
    t = primary_key_pair(table, primary_key_values)
    print(t)
    result = find_by_template(table, t, fields, limit, offset)
    return result


def find_by_template(table, t, fields=None, limit=None, offset=None):
    where = template_to_where(t)
    if fields is None:
        sql = "SELECT *"
    else:
        sql = "SELECT " + ",".join(fields)
    sql = sql + " " + "FROM " + table + " " + where
    if limit is not None:
        sql = sql + " LIMIT " + limit[0] + " OFFSET " + offset[0]
    result = run_q(sql, None, True)
    return result


def insert(table, r):
    key = "(" + ",".join(list(r.keys())) + ")"
    value = "("
    for i in r.values():
        value = value + "'" + i + "'" + ","
    value = value[:-1] + ")"
    sql = "INSERT INTO " + table + " " + key + " VALUES " + value
    print(sql)
    result = run_q(sql, None, True)
    return result

def delete(table, primary_key_values):
    # sql_find_primary = "SHOW KEYS FROM " + table + " WHERE Key_name = 'PRIMARY'"
    # key_name = run_q(sql_find_primary, None, True)
    # key_columns = [k["Column_name"] for k in key_name]
    # t = dict(zip(key_columns, [primary_key_values]))
    t = primary_key_pair(table, primary_key_values)
    where = template_to_where(t)
    sql = "DELETE FROM " + table+ " " + where
    result = run_q(sql, None, True)
    return result

def update(table, primary_key_values, r):
    set0 = ""
    for (k,v) in r.items():
        set0 = set0 + k + "=" + "'" + v + "'" + ", "
    set = set0[:-2]
    # sql_find_primary = "SHOW KEYS FROM " + table + " WHERE Key_name = 'PRIMARY'"
    # key_name = run_q(sql_find_primary, None, True)
    # key_columns = [k["Column_name"] for k in key_name]
    # t = dict(zip(key_columns, [primary_key_values]))
    t = primary_key_pair(table, primary_key_values)
    where = template_to_where(t)
    sql = "UPDATE " + table + " SET " + set + " " + where
    result = run_q(sql, None, True)
    return result


def join(primary_key,table1,table2):
    sql_find_primary = "SHOW KEYS FROM " + table1 + " WHERE Key_name = 'PRIMARY'"
    key_name = run_q(sql_find_primary, None, True)
    key_columns = [k["Column_name"] for k in key_name]
    sql_join = table1 + " JOIN " + table2 + "on" + table1 + "." + key_columns + "=" \
               + table1 + "." + key_columns
    print(sql_join)


def search_teammate(playerid):
    sql = "SELECT batting1.playerID AS ID, batting2.playerID AS names, MIN(batting2.yearID) AS first_year,\
           MAX(batting2.yearID) AS last_year, COUNT(*) AS times \
           FROM batting batting2 JOIN batting batting1 \
           ON batting1.teamID=batting2.teamID AND batting1.yearID=batting2.yearID \
           WHERE batting1.playerid='" + playerid + "' GROUP BY batting2.playerID"
    result = run_q(sql, None, True)
    return result

def career_stats(playerid):
    sql = "SELECT m.playerID AS playerid, m.teamID As teamid, m.yearID AS yearid, m.G AS g_all,\
    m.H AS hits, m.AB AS ABs, sum(n.A) as Assists, sum(n.E) as error \
    FROM Batting m JOIN Fielding n ON m.playerID=n.playerID and m.teamID=n.teamID and m.yearID=n.yearID \
    WHERE m.playerID='" + playerid + "' GROUP BY m.playerID, m.teamID, m.yearID, m.G, m.H, m.AB ORDER BY m.yearID"
    result = run_q(sql, None, True)
    return result

def roster(teamid, yearid):
    sql = "SELECT a.nameLast, a.nameFirst, a.playerID, b.teamid, b.yearID, b.g_all, b.hits, b.ABs, b.assists, b.errors FROM\
    (SELECT nameLast, nameFirst, playerID FROM people) AS a\
    JOIN\
    (SELECT m.playerID AS playerID, m.teamID AS teamid, m.yearID AS yearid, m.G AS g_all, m.H AS hits,\
    m.AB AS ABs, sum(n.A) as assists, sum(n.E) as errors\
    FROM Batting m JOIN Fielding n ON m.playerID=n.playerID and m.teamID=n.teamID and m.yearID=n.yearID\
    WHERE m.teamID='" + teamid + "'and m.yearID='" + yearid + "'\
    GROUP BY m.playerID, m.yearID, m.G, m.H, m.AB) AS b\
    on a.playerID = b.playerID"
    print(sql)
    result = run_q(sql, None, True)
    return result

def primary_key_pair(resource, primary_key_values):
    key_values = primary_key_values.split("_")
    sql_find_primary = "SHOW KEYS FROM " + resource + " WHERE Key_name = 'PRIMARY'"
    key_name = run_q(sql_find_primary, None, True)
    key_columns = [k["Column_name"] for k in key_name]
    t = dict(zip(key_columns, key_values))
    return t



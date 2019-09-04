from runners.helpers import log
from runners.helpers import db


def call_procedure(procedure, parameters):
    payload = None

    try:
        # call stored procedure
        if parameters is not None and len(parameters) > 0:
            params = "("
            for i in range(len(parameters)):
                params = params + "%s"
                if i < len(parameters) - 1:
                    params = params + ","
            params = params + ")"
        else:
            params = "()"

        sql = "call " + procedure + params

        log.debug(f"Procedure call sql {sql}")
        connection = db.connect()
        cur = connection.cursor()
        cur.execute(sql, tuple(parameters))
        rows = cur.fetchall()

        if len(rows) > 0:
            row = rows[0]

            if len(row) > 0:
                log.debug(f"Stored procedure  {procedure} response", ''.join(row[0]))
                payload = ''.join(row[0])

        cur.close()
    except Exception as e:
        log.error(f"Error executing stored procedure", e)
        raise

    return payload


def handle(alert, procedure=None, parameters=None):
    log.debug(f"Procedure name {procedure}")
    log.debug(f"Procedure parameters {parameters}")
    if procedure is not None:
        # call Snowflake stored procedure
        try:
            result = call_procedure(procedure, parameters)
            return result
        except Exception:
            return None
    else:
        return None

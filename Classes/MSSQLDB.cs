using Microsoft.Data.SqlClient;
using System.Data;

namespace Manifest.Report.Classes
{
    public class MSSQLDB : IDisposable
    {
        private readonly SqlConnection _connection;

        public MSSQLDB(SqlConnection connection)
        {
            _connection = connection;
        }

        public async Task<SqlDataReader> ExecuteReader(string sql, params SqlParameter[] parameters)
        {
            await EnsureConnectedAsync();
            var command = GetCommandWithParams(sql, parameters);
            return command.ExecuteReader(CommandBehavior.SequentialAccess);
        }

        public async Task<DataTable> ExecuteDataTableAsync(string sql, params SqlParameter[] parameters)
        {
            await EnsureConnectedAsync();
            var command = GetCommandWithParams(sql, parameters);

            using (var da = new SqlDataAdapter(command))
            {
                DataTable dt = new DataTable();

                da.Fill(dt);

                return dt;
            }
        }

        public DataTable ExecuteDataTable(string sql, params SqlParameter[] parameters)
        {
            EnsureConnected();
            var command = GetCommandWithParams(sql, parameters);

            using (var da = new SqlDataAdapter(command))
            {
                DataTable dt = new DataTable();

                da.Fill(dt);

                return dt;
            }
        }

        public async Task<List<T>> ExecuteListAsync<T>(string sql, params SqlParameter[] parameters)
        {
            var dt = await ExecuteDataTableAsync(sql, parameters);

            List<T> items = new List<T>();

            foreach (DataRow row in dt.Rows)
            {
                T item = (T)Activator.CreateInstance(typeof(T), row);
                items.Add(item);
            }

            return items;
        }

        public List<T> ExecuteList<T>(string sql, params SqlParameter[] parameters)
        {
            var dt = ExecuteDataTable(sql, parameters);

            List<T> items = new List<T>();

            foreach (DataRow row in dt.Rows)
            {
                T item = (T)Activator.CreateInstance(typeof(T), row);
                items.Add(item);
            }

            return items;
        }

        public async Task<T> ExecuteSingleRowAsync<T>(string sql, params SqlParameter[] parameters)
        {
            var rows = await ExecuteListAsync<T>(sql, parameters);

            return rows.FirstOrDefault();
        }

        public T ExecuteSingleRow<T>(string sql, params SqlParameter[] parameters)
        {
            var rows = ExecuteList<T>(sql, parameters);

            return rows.FirstOrDefault();
        }

        public async Task<int> ExecuteNonQueryAsync(string sql, params SqlParameter[] parameters)
        {
            await EnsureConnectedAsync();
            var command = GetCommandWithParams(sql, parameters);

            return await command.ExecuteNonQueryAsync();
        }

        public int ExecuteNonQuery(string sql, params SqlParameter[] parameters)
        {
            EnsureConnected();
            var command = GetCommandWithParams(sql, parameters);

            return command.ExecuteNonQuery();
        }

        public async Task<T> ExecuteScalarAsync<T>(string sql, params SqlParameter[] parameters)
        {
            await EnsureConnectedAsync();
            var command = GetCommandWithParams(sql, parameters);

            var retValue = await command.ExecuteScalarAsync();

            if (retValue is T)
                return (T)retValue;

            return default;
        }

        private SqlCommand GetCommandWithParams(string sql, SqlParameter[] parameters)
        {
            var command = _connection.CreateCommand();
            command.CommandTimeout = 300;
            command.CommandText = sql;
            command.Parameters.AddRange(parameters);

            return command;
        }

        private async Task EnsureConnectedAsync()
        {
            if (_connection.State == ConnectionState.Closed)
            {
                await _connection.OpenAsync();
            }
        }

        private void EnsureConnected()
        {
            if (_connection.State == ConnectionState.Closed)
            {
                _connection.Open();
            }
        }

        public void Dispose()
        {
            _connection?.Close();
            _connection?.Dispose();
        }

        public void Dispose(bool isDisposing)
        {
            if (isDisposing) Dispose();
        }
    }
}

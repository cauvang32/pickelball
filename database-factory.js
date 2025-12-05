import PickleballDatabaseSQLite from './database.js'
import PickleballDatabasePostgreSQL from './database-postgresql.js'
import dotenv from 'dotenv'

dotenv.config()

class PickleballDatabaseFactory {
  static async create() {
    const dbType = process.env.DB_TYPE || 'sqlite'
    
    if (dbType === 'postgresql') {
      console.log('🐘 Initializing PostgreSQL database...')
      const db = new PickleballDatabasePostgreSQL()
      await db.init()
      return db
    } else {
      console.log('🗃️ Initializing SQLite database...')
      const db = new PickleballDatabaseSQLite()
      await db.init()
      return db
    }
  }
}

export default PickleballDatabaseFactory

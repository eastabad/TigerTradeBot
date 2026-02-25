import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import * as schema from "@shared/schema";

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL must be set. Did you forget to provision a database?",
  );
}

export const pool = new Pool({ connectionString: process.env.DATABASE_URL });
export const db = drizzle(pool, { schema });

export async function runMigrations(): Promise<void> {
  console.log("Running database migrations...");
  
  try {
    await pool.query(`
      ALTER TABLE trades ADD COLUMN IF NOT EXISTS position_entry_date TIMESTAMP;
    `);
    console.log("Migration: position_entry_date column ensured");
    
    await pool.query(`
      ALTER TABLE trades ADD COLUMN IF NOT EXISTS filled_at TIMESTAMP;
    `);
    console.log("Migration: filled_at column ensured");
    
    console.log("Database migrations completed successfully");
  } catch (error) {
    console.error("Migration error:", error);
    throw error;
  }
}

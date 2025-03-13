import os
import snowflake.connector
from jinja2 import Environment, FileSystemLoader

# Establish Snowflake connection
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role="ACCOUNTADMIN"
)
cursor = conn.cursor()

# Check if CO2_S3_INTEGRATION exists
cursor.execute("SHOW STORAGE INTEGRATIONS;")
integrations = cursor.fetchall()
existing_integrations = [row[0] for row in integrations]

integration_exists = "CO2_S3_INTEGRATION" in existing_integrations

# Close connection
cursor.close()
conn.close()

# Set up Jinja environment
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

# Load the SQL Jinja template
template = env.get_template("snowflake_queries.sql.j2")

# Define dynamic values for placeholders
context = {
    "aws_role_arn": "arn:aws:iam::183295451617:role/MySnowflakeRole",
    "s3_bucket": "s3://big.data.ass3/",
    "integration_exists": integration_exists,  # Pass whether integration exists
}

# Render the SQL query with actual values
rendered_sql = template.render(context)

# Save the rendered SQL to a file
output_file = os.path.join(os.path.dirname(__file__), "sql", "generated_snowflake_queries.sql")
with open(output_file, "w") as f:
    f.write(rendered_sql)

print(f"✅ SQL file generated: {output_file}")

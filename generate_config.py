from jinja2 import Template

# Define the Jinja template for Snowflake configurations
jinja_template = """
USE ROLE {{ ACCOUNTADMIN }};
USE WAREHOUSE {{ COMPUTE_WH }};
USE DATABASE {{ CO2_DB}};
USE SCHEMA {{ RAW_DATA }};

CREATE TABLE IF NOT EXISTS {{RAW_DATA }}.DOW30_HARMONIZED (
    date DATE,
    co2_ppm FLOAT,
    daily_change FLOAT,
    PRIMARY KEY (date)
);
"""

# Define parameters for DEV and PROD environments
configurations = {
    "dev": {
        "role": "DEV_ROLE",
        "warehouse": "DEV_WAREHOUSE",
        "database": "CO2_DB",
        "schema": "DEV_SCHEMA"
    },
    "prod": {
        "role": "PROD_ROLE",
        "warehouse": "PROD_WAREHOUSE",
        "database": "CO2_DB",
        "schema": "PROD_SCHEMA"
    }
}

# Generate SQL files for both environments
for env, params in configurations.items():
    template = Template(jinja_template)
    sql_script = template.render(params)
    
    # Save the output to a SQL file
    with open(f"config_{env}.sql", "w") as f:
        f.write(sql_script)

    print(f"Generated config_{env}.sql successfully!")
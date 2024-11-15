# My Tool User Guide

## Overview

`my_tool` is a command-line interface (CLI) tool designed to interact with a database. It supports various operations such as creating, reading, updating, deleting records, transforming CSV files into a database format, and executing general queries.

## Installation

To use `my_tool`, follow these steps:

1. **Clone the Repository**:

   ```bash
    git clone https://github.com/nogibjj/Allen_Wang_miniproj_7.git
    cd Allen_Wang_miniproj_7
   ```

2. **Install Dependencies**:

   You can run the project as an executable by

   ```bash
   python setup.py develop
   ```

## Usage

Once installed, you can use `my_tool` from the command line. The basic syntax is:

```bash
my_tool <action> [<args>]
```

### Available Actions

1. **Create**: Add a new record to the database.
   ```bash
   my_tool create <country> <beer_servings> <spirit_servings> <wine_servings> <total_litres_of_pure_alcohol>
   ```

   - **Arguments**:
     - `country`: The name of the country.
     - `beer_servings`: Number of beer servings.
     - `spirit_servings`: Number of spirit servings.
     - `wine_servings`: Number of wine servings.
     - `total_litres_of_pure_alcohol`: Total litres of pure alcohol.

2. **Read**: Retrieve and display all records from the database.
   ```bash
   my_tool read
   ```

3. **Update**: Update the number of beer servings for a specified country.
   ```bash
   my_tool update <country> <beer_servings>
   ```

   - **Arguments**:
     - `country`: The name of the country.
     - `beer_servings`: New number of beer servings.

4. **Delete**: Remove a record from the database.
   ```bash
   my_tool delete <country>
   ```

   - **Arguments**:
     - `country`: The name of the country.

5. **Transform**: Transform 2 URLs of CSV files into the database format.
   ```bash
   my_tool transform <url1> <url2>
   ```

   - **Arguments**:
     - `url1`: The first URL of the CSV file.
     - `url2`: The second URL of the CSV file.

6. **General**: Execute a custom SQL query.
   ```bash
   my_tool general <query>
   ```

   - **Arguments**:
     - `query`: The SQL query to execute.

## Example Commands

- To create a new record for Namibia:
  ```bash
  my_tool create Namibia 376 15 12 12.2
  ```

- To read all records:
  ```bash
  my_tool read
  ```

- To update the beer servings for Germany:
  ```bash
  my_tool update Germany 400
  ```

- To delete a record for Gabon:
  ```bash
  my_tool delete Gabon
  ```

- To transform CSV files:
  ```bash
  my_tool transform https://example.com/source.csv https://example.com/source2.csv
  ```

- To run a general query:
  ```bash
  my_tool general "SELECT * FROM zw308_drink WHERE country = 'Czech Republic'"
  ```

# Data Exploration

The following potential anomalies were found during data exploration.

1. **Null values**

   * `vw_raw_orders`: `order_date`, `currency`
   * `vw_raw_customers`: `email`, `phone`

2. **Negative values**

   * `total_amount` in `vw_raw_orders` contains negative values.

3. **Missing customer references**

   * Some `customer_id` values in `vw_raw_orders` do not exist in `vw_raw_customers`.

4. **Duplicate records**

   * Duplicate `customer_id` values were found in `vw_raw_customers`.

## How to Run

Install packages first.

```bash
pip install pandas
pip install prefect
pip install pytest
```

Start Prefect server.

```bash
prefect server start
```

Open another terminal and start worker.

```bash
prefect worker start --pool WORK_POOL_NAME
```

If needed, run:

```bash
prefect profile populate-defaults
```

Deploy the flow.

```bash
prefect deploy
```

Check deployment.

```bash
prefect deployment inspect FLOW_NAME/DEPLOYMENT_NAME
```

If the worker and deployment status are **Ready**, open Prefect UI.

```text
http://127.0.0.1:4200
```

Go to the deployment `FLOW_NAME/DEPLOYMENT_NAME` and click **Quick Run** to start
the pipeline.

Replace the following placeholders with names from your Prefect configuration:

- `WORK_POOL_NAME`: the work pool name, such as `default`
- `FLOW_NAME`: the registered flow name, such as `main-etl`
- `DEPLOYMENT_NAME`: the deployment name, such as `default`

## Tests

The unit tests use mock DataFrames

Open a terminal in the project directory and run all tests in
`test_pipeline.py`:

```bash
python -m pytest test_pipeline.py -v
```

Run a specific test class:

```bash
python -m pytest test_pipeline.py::TestClassName -v
```

Run a specific test function:

```bash
python -m pytest test_pipeline.py::TestClassName::test_function_name -v
```

Replace `TestClassName` and `test_function_name` with the names declared in
`test_pipeline.py`.

When all tests pass, the output will look similar to this:

```text
12 passed
```

# digdag_operator_snowflake

Digdag `snow>` operator plugin to execute a query Snowflake.

## 設定

### 設定例

```yaml
_export:
  plugin:
    # repositories:
    #   - file:///Users/h_hori/.m2/repository/
    # dependencies:
    #   - dev.hiro-hori:digdag-operator-snowflake_2.13:0.1.0-SNAPSHOT
    repositories:
      - https://jitpack.io
    dependencies:
      - com.github.flinters:digdag_operator_snowflake:v0.1.3
  snow:
    host: zz99999.us-east-99.aws.snowflakecomputing.com
    # role: sysadmin
    user: snow_user
    warehouse: compute_wh
    # schema: public
    # query_tag: etl_task_for_xyz123

+run_task:
  snow>: example.sql
  # 設定可能箇所 - snow>の直下とはここを指す
  database: TEST_HORI
  schema: public
  # create_table: hogehoge
  create_or_replace_table: hogehoge
  # create_table_if_not_exists: hogehoge
  # insert_into: hogehoge
```

### パラメータ一覧
parameter名|必須？|補足|設定可能箇所<br>snow>の直下|設定可能箇所<br>exportされた`snow.{parameter}`|設定可能箇所<br>シークレット
---|---|---|---|---|---
`host`|o|Snowflake環境のホスト名|o|o|x
`user`|o|Snowflake接続ユーザー名|o|o|x
`snow.password`|o|Snowflake接続パスワード|x|x|o
`role`|x|Snowflakeの接続ロール名|o|o|x
`warehouse`|x|演算が行われる、Snowflakeのウェアハウス名|o|o|x
`database`|x|セッションに使われるデータベース|o|o|x
`query_tag`|x|Snowflakeのクエリタグ名|o|o|x
`schema`|x|セッションに使われるスキーマ|o|o|x
`create_table`|x|クエリの冒頭にCREATE TABLE {table} AS を付与|o|x|x
`create_or_replace_table`|x|クエリの冒頭にCREATE OR REPLACE TABLE {table} AS を付与|o|x|x
`create_table_if_not_exists`|x|クエリの冒頭にCREATE TABLE {table} IF NOT EXISTS AS を付与|o|x|x
`insert_into`|x|クエリの冒頭にINSERT INTO {table} AS を付与|o|x|x
`session_unixtime_sql_variable_name`|x|digdagのsession_unixtimeを、Snowflake SQL変数にsetする。その際の変数名|o|o|x

snow>の直下およびexportされた`snow.{parameter}`両方に設定可能なパラメータが、両方に設定されていた場合は、snow>の直下に設定された値を優先して使用する

### `snow.password`シークレット設定例

Register Snowflake password into secrets.

local mode
```
digdag secrets --local --set snow.password
```

server mode
```
digdag secrets --project <project> --set snow.password
```

## 開発

### 1) build

```sh
sbt publishM2
```

Artifacts are build on local repos: `~/.m2`.

### 2) run an example

```sh
rm -rf .digdag/plugins 
digdag run example.dig --session daily -a
```

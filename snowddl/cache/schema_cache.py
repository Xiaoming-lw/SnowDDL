from typing import TYPE_CHECKING
from importlib.util import module_from_spec, spec_from_file_location

from snowddl.blueprint import Ident

if TYPE_CHECKING:
    from snowddl.engine import SnowDDLEngine


class SchemaCache:
    def __init__(self, engine: "SnowDDLEngine"):
        self.engine = engine

        self.db_filter = []
        self.schema_filter = []
        self.load_filter()

        self.databases = {}
        self.schemas = {}

        self.reload()

    def load_filter(self):
        for module_path in sorted(self.engine.config.config_path.parent.glob("filter/*.py")):
            try:
                spec = spec_from_file_location(module_path.name, module_path)
                module = module_from_spec(spec)
                spec.loader.exec_module(module)

                if hasattr(module, "db_filter"):
                    self.db_filter.append(module.db_filter)
                if hasattr(module, "schema_filter"):
                    self.schema_filter.append(module.schema_filter)
            except Exception as e:
                raise RuntimeError("Load custom filter files failed.") from e

    def reload(self):
        self.databases = {}
        self.schemas = {}

        cur = self.engine.execute_meta(
            "SHOW DATABASES LIKE {env_prefix:ls}",
            {
                "env_prefix": self.engine.config.env_prefix,
            },
        )

        try:
            for f in self.db_filter:
                cur = f(cur)
        except Exception as e:
            raise RuntimeError("Run custom db filter failed.") from e

        for r in cur:
            # Skip databases created by other roles
            if r["owner"] != self.engine.context.current_role and not self.engine.settings.ignore_ownership:
                continue

            # Skip shares
            if r["origin"]:
                continue

            # Skip databases not listed in settings explicitly
            if self.engine.settings.include_databases and Ident(r["name"]) not in self.engine.settings.include_databases:
                continue

            self.databases[r["name"]] = {
                "database": r["name"],
                "owner": r["owner"],
                "comment": r["comment"] if r["comment"] else None,
                "is_transient": "TRANSIENT" in r["options"],
                "retention_time": int(r["retention_time"]),
            }

        # Process schemas in parallel
        for database_schemas in self.engine.executor.map(self._get_database_schemas, self.databases):
            self.schemas.update(database_schemas)

    def _get_database_schemas(self, database_name):
        schemas = {}

        cur = self.engine.execute_meta(
            "SHOW SCHEMAS IN DATABASE {database:i}",
            {
                "database": database_name,
            },
        )

        try:
            for f in self.schema_filter:
                cur = f(cur)
        except Exception as e:
            raise RuntimeError("Run custom schema filter failed.") from e

        for r in cur:
            # Skip INFORMATION_SCHEMA
            if r["name"] == "INFORMATION_SCHEMA":
                continue

            schemas[f"{r['database_name']}.{r['name']}"] = {
                "database": r["database_name"],
                "schema": r["name"],
                "owner": r["owner"],
                "comment": r["comment"] if r["comment"] else None,
                "is_transient": "TRANSIENT" in r["options"],
                "is_managed_access": "MANAGED ACCESS" in r["options"],
                "retention_time": int(r["retention_time"]) if r["retention_time"].isdigit() else 0,
            }

        return schemas

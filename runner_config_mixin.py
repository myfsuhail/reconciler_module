"""
Configuration normalization helpers for ValidationRunner.
"""

from typing import Any


class RunnerConfigMixin:
    """Mixin for parsing/normalizing input table configs."""

    def _normalize_table_configs(
        self, table_configs: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Normalize mixed input shapes into legacy per-table config format."""
        normalized: list[dict[str, Any]] = []

        for idx, config in enumerate(table_configs, 1):
            if not isinstance(config, dict):
                raise ValueError(f"table_configs[{idx - 1}] must be a dict")

            recon_type = config.get("recon_type")
            if recon_type is None:
                normalized.append(config)
                continue

            recon_type_str = str(recon_type).strip().lower()
            if recon_type_str == "basic":
                normalized.extend(self._expand_basic_recon_config(config, idx))
            elif recon_type_str == "dq":
                normalized.append(self._expand_dq_recon_config(config, idx))
            else:
                raise ValueError(
                    f"Unsupported recon_type '{recon_type}' in table_configs[{idx - 1}]. "
                    f"Supported values are: 'basic', 'dq'"
                )

        return normalized

    def _expand_basic_recon_config(
        self, config: dict[str, Any], config_index: int
    ) -> list[dict[str, Any]]:
        """Expand compact basic recon config into per-table legacy configs."""
        self._require_keys(
            config,
            required_keys=["source_database", "target_database", "tables_list"],
            context=f"table_configs[{config_index - 1}]",
        )

        tables_list = config["tables_list"]
        if not isinstance(tables_list, list) or not tables_list:
            raise ValueError(
                f"table_configs[{config_index - 1}].tables_list must be a non-empty list"
            )

        expanded: list[dict[str, Any]] = []
        for table_idx, table_spec in enumerate(tables_list, 1):
            source_table, target_table, skip_columns = self._parse_table_mapping_spec(
                table_spec,
                config_index=config_index,
                table_index=table_idx,
            )

            expanded.append(
                {
                    "source_database": config["source_database"],
                    "source_table": source_table,
                    "target_database": config["target_database"],
                    "target_table": target_table,
                    "auto_tc1": {
                        "type": "count_check",
                        "is_enabled": True,
                        "tolerance_in_percent": 0.0,
                    },
                    "auto_tc2": {
                        "type": "column_name_check",
                        "is_enabled": True,
                        "skip_columns": skip_columns,
                    },
                    "auto_tc3": {
                        "type": "column_datatype_check",
                        "is_enabled": True,
                        "skip_columns": skip_columns,
                    },
                }
            )

        return expanded

    def _expand_dq_recon_config(
        self, config: dict[str, Any], config_index: int
    ) -> dict[str, Any]:
        """Expand compact dq recon config and enforce required basic validations."""
        self._require_keys(
            config,
            required_keys=[
                "source_database",
                "source_table",
                "target_database",
                "target_table",
            ],
            context=f"table_configs[{config_index - 1}]",
        )

        normalized: dict[str, Any] = {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
        }

        for key, value in config.items():
            if key in {"recon_type", *self.TABLE_ID_KEYS}:
                continue
            normalized[key] = value

        return self._ensure_required_basic_validations(normalized)

    def _ensure_required_basic_validations(
        self, config: dict[str, Any]
    ) -> dict[str, Any]:
        """Ensure count/name/datatype checks exist and count tolerance is always 0."""
        normalized: dict[str, Any] = {
            "source_database": config["source_database"],
            "source_table": config["source_table"],
            "target_database": config["target_database"],
            "target_table": config["target_table"],
        }

        existing_types: set[str] = set()

        for key, value in config.items():
            if key in self.TABLE_ID_KEYS:
                continue

            if isinstance(value, dict) and isinstance(value.get("type"), str):
                validation_type = value["type"].strip()
                adjusted = dict(value)

                if validation_type == "count_check":
                    adjusted["is_enabled"] = True
                    adjusted["tolerance_in_percent"] = 0.0
                elif validation_type in {"column_name_check", "column_datatype_check"}:
                    adjusted["is_enabled"] = True

                normalized[key] = adjusted
                existing_types.add(validation_type)
            else:
                normalized[key] = value

        if "count_check" not in existing_types:
            normalized["auto_tc1"] = {
                "type": "count_check",
                "is_enabled": True,
                "tolerance_in_percent": 0.0,
            }

        if "column_name_check" not in existing_types:
            normalized["auto_tc2"] = {
                "type": "column_name_check",
                "is_enabled": True,
                "skip_columns": [],
            }

        if "column_datatype_check" not in existing_types:
            normalized["auto_tc3"] = {
                "type": "column_datatype_check",
                "is_enabled": True,
                "skip_columns": [],
            }

        return normalized

    def _parse_table_mapping_spec(
        self,
        table_spec: Any,
        config_index: int,
        table_index: int,
    ) -> tuple[str, str, list[str]]:
        """Parse table spec: table, source:target, source:target(skip1, skip2)."""
        context = f"table_configs[{config_index - 1}].tables_list[{table_index - 1}]"

        if not isinstance(table_spec, str):
            raise ValueError(f"{context} must be a string")

        raw_spec = table_spec.strip()
        if not raw_spec:
            raise ValueError(f"{context} cannot be empty")

        open_count = raw_spec.count("(")
        close_count = raw_spec.count(")")
        if open_count != close_count or open_count > 1:
            raise ValueError(
                f"Invalid table spec '{table_spec}' at {context}. "
                "Expected formats: 'table', 'source:target', or 'source:target(col1, col2)'"
            )

        skip_columns: list[str] = []
        base_spec = raw_spec
        if open_count == 1:
            if not raw_spec.endswith(")"):
                raise ValueError(
                    f"Invalid table spec '{table_spec}' at {context}. "
                    "Skip-column section must end with ')'"
                )

            base_spec, columns_part = raw_spec.split("(", 1)
            base_spec = base_spec.strip()
            columns_part = columns_part[:-1]
            skip_columns = self._parse_skip_columns(columns_part, table_spec, context)

        if ":" in base_spec:
            mapping_parts = base_spec.split(":")
            if len(mapping_parts) != 2:
                raise ValueError(
                    f"Invalid table spec '{table_spec}' at {context}. "
                    "Only one ':' is allowed"
                )
            source_table = mapping_parts[0].strip()
            target_table = mapping_parts[1].strip()
        else:
            source_table = base_spec.strip()
            target_table = base_spec.strip()

        if not source_table or not target_table:
            raise ValueError(
                f"Invalid table spec '{table_spec}' at {context}. "
                "Source and target table names must be non-empty"
            )

        return source_table, target_table, skip_columns

    def _parse_skip_columns(
        self, columns_part: str, table_spec: str, context: str
    ) -> list[str]:
        """Parse and validate skip column list."""
        if not columns_part.strip():
            raise ValueError(
                f"Invalid table spec '{table_spec}' at {context}. "
                "Skip-column list cannot be empty when parentheses are provided"
            )

        columns = [c.strip() for c in columns_part.split(",")]
        if any(not c for c in columns):
            raise ValueError(
                f"Invalid table spec '{table_spec}' at {context}. "
                "Skip-column list contains an empty column name"
            )

        return columns

    @staticmethod
    def _require_keys(
        config: dict[str, Any], required_keys: list[str], context: str
    ) -> None:
        """Raise clear error when required keys are missing."""
        missing = [key for key in required_keys if key not in config]
        if missing:
            raise ValueError(f"Missing required key(s) in {context}: {missing}")

from snowddl.blueprint import TagBlueprint, Ident, IdentWithPrefix, ComplexIdentWithPrefix, ObjectType, TagReference
from snowddl.parser.abc_parser import AbstractParser


tag_json_schema = {
    "type": "object",
    "properties": {
        "references": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "object_type": {
                        "type": "string"
                    },
                    "object_name": {
                        "type": "string"
                    },
                    "column_name": {
                        "type": "string"
                    },
                    "tag_value": {
                        "type": "string"
                    }
                },
                "required": ["object_type", "object_name", "tag_value"],
                "additionalProperties": False
            },
            "minItems": 1
        },
        "comment": {
            "type": "string"
        }
    },
    "additionalProperties": False
}


class TagParser(AbstractParser):
    def load_blueprints(self):
        for f in self.parse_schema_object_files("tag", tag_json_schema):
            references = []

            for a in f.params.get('references', []):
                ref = TagReference(
                    object_type=ObjectType[a['object_type'].upper()],
                    object_name=self.config.build_complex_ident(a['object_name'], f.database, f.schema),
                    column_name=Ident(a['column_name']) if a.get('column_name') else None,
                    tag_value=a['tag_value'],
                )

                references.append(ref)

            bp = TagBlueprint(
                full_name=ComplexIdentWithPrefix(self.env_prefix, f.database, f.schema, f.name),
                database=IdentWithPrefix(self.env_prefix, f.database),
                schema=Ident(f.schema),
                name=Ident(f.name),
                references=references,
                comment=f.params.get('comment'),
            )

            self.config.add_blueprint(bp)

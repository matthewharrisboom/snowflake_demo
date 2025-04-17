import sys
import yaml
import os
from jinja2 import Environment, FileSystemLoader

def render_all_sql_templates(environment_name):
    # Load the Jinja2 environment and context
    env = Environment(loader=FileSystemLoader("templates"))

    with open(f"configs/{environment_name}.yml") as f:
        context = yaml.safe_load(f)

    # Output directory
    os.makedirs("rendered", exist_ok=True)

    # Loop through all .sql.j2 templates
    for template_name in os.listdir("templates"):
        if not template_name.endswith(".sql.j2"):
            continue

        template = env.get_template(template_name)
        rendered = template.render(**context)

        base_name = template_name.replace(".sql.j2", f"_{environment_name}.sql")
        output_path = os.path.join("rendered", base_name)

        with open(output_path, "w") as f:
            f.write(rendered)

        print(f"Rendered: {output_path}")

if __name__ == "__main__":
    env_name = sys.argv[1]
    render_all_sql_templates(env_name)
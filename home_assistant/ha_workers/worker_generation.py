import os
import yaml
import copy

# ---------------------------------------------------------
# 1. DOMAIN DEFINITIONS
# ---------------------------------------------------------

DOMAINS = {
    "temperature": {
        "prefix": "temp",
        "count": 100,
        "min": -10,
        "max": 120,
        "step": 0.1,
        "unit": "°F",
        "drift_id": "fake_temperature_drift",
        "drift_formula": "{{ 70 + 3 * sin((now().timestamp() / 300) + (loop.index0 * 0.1)) + (range(-10, 10) | random / 10) }}",
    },
    "humidity": {
        "prefix": "hum",
        "count": 100,
        "min": 0,
        "max": 100,
        "step": 0.1,
        "unit": "%",
        "drift_id": "fake_humidity_drift",
        "drift_formula": "{{ 45 + (range(-5, 5) | random) }}",
    },
    "power": {
        "prefix": "power",
        "count": 100,
        "min": 0,
        "max": 5000,
        "step": 5,
        "unit": "W",
        "drift_id": "fake_power_drift",
        "drift_formula": "{{ 200 + 80 * sin(now().timestamp() / 250) + (range(-50, 50) | random) }}",
    },
    "light": {
        "prefix": "light",
        "count": 100,
        "min": 0,
        "max": 1000,
        "step": 1,
        "unit": "lx",
        "drift_id": "fake_light_drift",
        "drift_formula": "{{ 300 + 140 * sin(now().timestamp() / 800) + (range(-30, 40) | random) }}",
    },
    "co2": {
        "prefix": "co2",
        "count": 100,
        "min": 400,
        "max": 2000,
        "step": 1,
        "unit": "ppm",
        "drift_id": "fake_co2_drift",
        "drift_formula": "{{ 600 + 80 * sin(now().timestamp() / 450) + (range(-25, 25) | random) }}",
    },
    "noise": {
        "prefix": "noise",
        "count": 100,
        "min": 20,
        "max": 120,
        "step": 1,
        "unit": "dB",
        "drift_id": "fake_noise_drift",
        "drift_formula": "{{ 40 + 5 * sin(now().timestamp() / 90) + (range(-5, 15) | random) }}",
    },
    "occupancy": {
        "prefix": "occ",
        "count": 100,
        "min": 0,
        "max": 1,
        "step": 0.01,
        "unit": None,
        "drift_id": "fake_occupancy_drift",
        "drift_formula": "{{ (now().hour >= 7 and now().hour <= 22) | ternary(0.7 + (range(-10, 10) | random) / 100, 0.2 + (range(-10, 10) | random) / 100) }}",
    },
    "hvac": {
        "prefix": "hvac",
        "count": 100,
        "min": 0,
        "max": 3,
        "step": 1,
        "unit": "mode",
        "drift_id": "fake_hvac_drift",
        "drift_formula": "{{ 0 }}",
    },
}

# ---------------------------------------------------------
# 2. INTERACTION TEMPLATES
# ---------------------------------------------------------

INTERACTION_TEMPLATES = [
    {
        "alias": "Temp Affects HVAC (Cooling) __SUFFIX__",
        "id": "temp_controls_hvac_cool__SUFFIX__",
        "trigger": [
            {
                "platform": "numeric_state",
                "entity_id": "input_number.temp__SUFFIX__",
                "above": 76,
            }
        ],
        "action": [
            {
                "service": "input_number.set_value",
                "target": {"entity_id": "input_number.hvac__SUFFIX__"},
                "data_template": {"value": 1},
            }
        ],
    },
    {
        "alias": "Temp Affects HVAC (Heating) __SUFFIX__",
        "id": "temp_controls_hvac_heat__SUFFIX__",
        "trigger": [
            {
                "platform": "numeric_state",
                "entity_id": "input_number.temp__SUFFIX__",
                "below": 68,
            }
        ],
        "action": [
            {
                "service": "input_number.set_value",
                "target": {"entity_id": "input_number.hvac__SUFFIX__"},
                "data_template": {"value": 2},
            }
        ],
    },
    {
        "alias": "Occupancy Controls Illumination __SUFFIX__",
        "id": "occ_controls_light__SUFFIX__",
        "trigger": [{"platform": "state", "entity_id": "input_number.occ__SUFFIX__"}],
        "action": [
            {
                "service": "input_number.set_value",
                "target": {"entity_id": "input_number.light__SUFFIX__"},
                "data_template": {
                    "value": "{{ (states('input_number.occ__SUFFIX__') | float * 900) + (range(-20, 20) | random) }}"
                },
            }
        ],
    },
    {
        "alias": "CO2 Drives Power Ventilation __SUFFIX__",
        "id": "co2_controls_power__SUFFIX__",
        "trigger": [
            {
                "platform": "numeric_state",
                "entity_id": "input_number.co2__SUFFIX__",
                "above": 1000,
            }
        ],
        "action": [
            {
                "service": "input_number.set_value",
                "target": {"entity_id": "input_number.power__SUFFIX__"},
                "data_template": {"value": "{{ 300 + (range(0,50) | random) }}"},
            }
        ],
    },
    {
        "alias": "Noise Influences Occupancy Confidence __SUFFIX__",
        "id": "noise_controls_occ__SUFFIX__",
        "trigger": [{"platform": "state", "entity_id": "input_number.noise__SUFFIX__"}],
        "action": [
            {
                "service": "input_number.set_value",
                "target": {"entity_id": "input_number.occ__SUFFIX__"},
                "data_template": {
                    "value": "{{ 0.5 + (states('input_number.noise__SUFFIX__') | float - 40) * 0.005 + (range(-5,5) | random) / 100 }}"
                },
            }
        ],
    },
]

# ---------------------------------------------------------
# UTILITIES
# ---------------------------------------------------------

OUTPUT_DIR = "workers"


def write_yaml(path, data):
    with open(path, "w") as f:
        # width=1000 prevents line wrapping
        yaml.dump(
            data,
            f,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=1000,
        )


def write_raw(path, content):
    with open(path, "w") as f:
        f.write(content)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def build_looped_action(targets, value_formula):
    # Flatten the formula string
    clean_formula = value_formula.replace("\n", " ").strip()

    # Simple action for single target
    if isinstance(targets, str) or (isinstance(targets, list) and len(targets) == 1):
        t = targets[0] if isinstance(targets, list) else targets
        return [
            {
                "service": "input_number.set_value",
                "target": {"entity_id": t},
                "data_template": {"value": clean_formula},  # Changed to data_template
            }
        ]
    # Looped action for multiple targets
    return [
        {
            "repeat": {
                "for_each": targets,
                "sequence": [
                    {
                        "service": "input_number.set_value",
                        "target": {"entity_id": "{{ repeat.item }}"},
                        "data_template": {
                            "value": clean_formula
                        },  # Changed to data_template
                    }
                ],
            }
        }
    ]


def recursive_replace(obj, old, new):
    if isinstance(obj, dict):
        return {k: recursive_replace(v, old, new) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [recursive_replace(i, old, new) for i in obj]
    elif isinstance(obj, str):
        return obj.replace(old, new)
    else:
        return obj


# ---------------------------------------------------------
# MAIN GENERATOR
# ---------------------------------------------------------


def generate():
    print("Generating Instance-Isolated Workers Pack (using data_template)...")

    ensure_dir(OUTPUT_DIR)
    config_lines = []

    # ---------------------------
    # 1. GENERATE REGULAR DOMAINS
    # ---------------------------
    for domain, cfg in DOMAINS.items():
        domain_dir = os.path.join(OUTPUT_DIR, domain)
        ensure_dir(domain_dir)

        prefix = cfg["prefix"]
        count = cfg["count"]
        unit = cfg["unit"]

        all_entity_ids = []

        # A. input_numbers.yaml
        model_entries = {}
        for i in range(1, count + 1):
            eid = f"{prefix}_{i:03d}"
            full_entity_id = f"input_number.{eid}"
            all_entity_ids.append(full_entity_id)

            entry = {
                "name": f"{domain.capitalize()} {i}",
                "min": cfg["min"],
                "max": cfg["max"],
                "step": cfg["step"],
            }
            if unit:
                entry["unit_of_measurement"] = unit
            model_entries[eid] = entry

        write_yaml(os.path.join(domain_dir, "input_numbers.yaml"), model_entries)

        # B. customize.yaml
        customize_entries = {}
        for eid in all_entity_ids:
            customize_entries[eid] = {
                "friendly_name": f"{domain.capitalize()} {eid.split('_')[-1]}",
                "icon": "mdi:chart-line",
            }
        write_yaml(os.path.join(domain_dir, "customize.yaml"), customize_entries)

        # C. automation.yaml (Drift)
        drift_auto = [
            {
                "alias": f"Fake {domain.capitalize()} Drift",
                "id": cfg["drift_id"],
                "mode": "restart",
                "trigger": [{"platform": "time_pattern", "seconds": "/5"}],
                "action": build_looped_action(all_entity_ids, cfg["drift_formula"]),
            }
        ]
        write_yaml(os.path.join(domain_dir, "automation.yaml"), drift_auto)

        # D. package.yaml
        package_content = """
input_number: !include input_numbers.yaml
automation: !include automation.yaml
homeassistant:
  customize: !include customize.yaml
""".strip()
        write_raw(os.path.join(domain_dir, "package.yaml"), package_content)

        config_lines.append(
            f"    worker_{domain}: !include workers/{domain}/package.yaml"
        )
        print(f"✔ {domain} generated")

    # -------------------------------------------------------
    # 2. GENERATE CROSS DOMAIN (Instance-Aware)
    # -------------------------------------------------------
    cross_dir = os.path.join(OUTPUT_DIR, "cross_domain")
    ensure_dir(cross_dir)

    all_cross_automations = []
    count = 50

    print(
        f"  > Expanding {len(INTERACTION_TEMPLATES)} templates across {count} instances..."
    )

    for i in range(1, count + 1):
        suffix = f"_{i:03d}"
        for template in INTERACTION_TEMPLATES:
            new_auto = copy.deepcopy(template)
            new_auto = recursive_replace(new_auto, "__SUFFIX__", suffix)
            all_cross_automations.append(new_auto)

    write_yaml(os.path.join(cross_dir, "automation.yaml"), all_cross_automations)

    cross_package_content = """
automation: !include automation.yaml
""".strip()
    write_raw(os.path.join(cross_dir, "package.yaml"), cross_package_content)

    config_lines.append(
        f"    worker_cross_domain: !include workers/cross_domain/package.yaml"
    )
    print(
        f"✔ cross_domain generated ({len(all_cross_automations)} automations created)"
    )

    print(f"\n🎉 Done! Output saved to: {OUTPUT_DIR}/")
    print("\n---------------------------------------------------")
    print("Add this to your configuration.yaml:")
    print("---------------------------------------------------")
    print("homeassistant:")
    print("  packages:")
    for line in config_lines:
        print(line)


if __name__ == "__main__":
    generate()

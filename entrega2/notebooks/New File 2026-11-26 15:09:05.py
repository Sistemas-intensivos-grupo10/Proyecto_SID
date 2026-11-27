for d in range(1, 9):
    f = f"2023-06-0{d}"
    dbutils.notebook.run("./01_landing", 1800, {"fecha": f})
    print("landing OK", f)

    pasos = [
    ("./02_bronze",       {"verificar": "si"}),
    ("./03_land_mask",    {}),
    ("./04_silver",       {}),
    ("./05_gold",         {"fecha": "todas"}),
    ("./06_quality_gate", {"fecha": "2023-06-08"}),
]
for nb, params in pasos:
    print("▶", nb, params)
    dbutils.notebook.run(nb, 7200, params)
    print("✓", nb)
import argparse

from .itl import main


def load_itl(module_path):
    itl_module_path = module_path

    if ":" in itl_module_path:
        itl_module_path, itl_obj_name = itl_module_path.split(":")
    else:
        itl_obj_name = "app"

    itl_module = importlib.import_module(itl_module_path)
    itl = getattr(itl_module, itl_obj_name)

    return itl


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Your script description")
    parser.add_argument("module:app", help="Path to the itl module")
    args = parser.parse_args()

    itl = load_itl(args.itl)
    asyncio.run(main(itl, args))


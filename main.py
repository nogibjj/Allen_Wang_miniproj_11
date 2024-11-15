import sys
import argparse
from mylib.transform import csv_to_db
from mylib.query import update_row, delete_row, create_row, read_all, general


def handle_arguments(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["create", "read", "update", "delete", "transform", "general"]
    )
    args = parser.parse_args(args[:1])
    if args.action == "create":
        parser.add_argument("country")
        parser.add_argument("beer_servings", type=int)
        parser.add_argument("spirit_servings", type=int)
        parser.add_argument("wine_servings", type=int)
        parser.add_argument("total_litres_of_pure_alcohol")

    elif args.action == "update":
        parser.add_argument("country")
        parser.add_argument("beer_servings", type=int)

    elif args.action == "delete":
        parser.add_argument("country")

    elif args.action == "transform":
        parser.add_argument("url1")
        parser.add_argument("url2")

    elif args.action == "general":
        parser.add_argument("query")
    return parser.parse_args(sys.argv[1:])


def main():
    args = handle_arguments(sys.argv[1:])
    if args.action == "create":
        create_row(
            args.country,
            args.beer_servings,
            args.spirit_servings,
            args.wine_servings,
            args.total_litres_of_pure_alcohol,
        )
    elif args.action == "read":
        data = read_all()
        for row in data:
            print(row)
    elif args.action == "update":
        update_row(args.country, args.beer_servings)
    elif args.action == "delete":
        delete_row(args.country)
    elif args.action == "transform":
        csv_to_db(args.url1,args.url2)
    elif args.action == "general":
        print(1)
        data = general(args.query)
        if data:
            for row in data:
                print(row)
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()

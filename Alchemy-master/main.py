from typing import List

from sqlalchemy import create_engine, ForeignKey, select
from sqlalchemy.orm import mapped_column, declarative_base, Mapped, Session, relationship
import pandas as pd

engine = create_engine("sqlite:///db.db", echo=False)

df = pd.read_csv("IKEA_SA_Furniture_Web_Scrapings_sss.csv")

for i in df:
    if df[i].dtype == object:
        df = df.fillna({i: "Нет данных"})

# for i in df:
#     if df[i].dtype == float:
#         x = df[i].mean()
#         df = df.fillna({i: df[i].mean()})

# print(df1.iloc[0])
drop_box = ["Unnamed: 0", "link", "short_description"]
for item in drop_box:
    df = df.drop(item, axis=1)
# for i in df:
# print(i, df[i].dtype)

unique_values = df['category'].drop_duplicates(ignore_index=True)
id_i = pd.RangeIndex(len(unique_values))
value_to_id = dict(zip(unique_values, id_i))
df1 = pd.DataFrame({'category': unique_values, 'category_ID': id_i})
# print(df1.dtypes)

df['category_ID'] = df['category'].map(value_to_id)
df = df.drop('category', axis=1)
# print(df)
df = df.dropna()

Base = declarative_base()


# j = 0
# for row in df.to_dict(orient='records'):
#     print(row["item_id"])
#     j += 1
#     if j == 10:
#         break


# item_id = 1, name = "waf", price = 10, old_price = "wad",
# sellable_online = True, other_colors = "wafg", designer = "dwa", depth = 42, height = 41, width = 10,
# category_ID = 1

class Furniture(Base):
    __tablename__ = "furniture"  # noqa
    id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[int] = mapped_column()
    name: Mapped[str] = mapped_column()
    price: Mapped[float] = mapped_column()
    old_price: Mapped[str] = mapped_column()
    sellable_online: Mapped[bool] = mapped_column()
    other_colors: Mapped[str] = mapped_column()
    designer: Mapped[str] = mapped_column()
    depth: Mapped[float] = mapped_column()
    height: Mapped[float] = mapped_column()
    width: Mapped[float] = mapped_column()
    category_ID: Mapped[int] = mapped_column(ForeignKey("category.category_ID"))

    category: Mapped["Category"] = relationship(back_populates="furnitures")  # noqa


class Category(Base):
    __tablename__ = "category"  # noqa
    id: Mapped[int] = mapped_column(primary_key=True)
    category: Mapped[str] = mapped_column()
    category_ID: Mapped[int] = mapped_column()

    furnitures: Mapped[List["Furniture"]] = relationship(back_populates="category",  # noqa
                                                         cascade="all, delete-orphan")  # noqa


# furniture = FurnitureTable(item_id=1, name="waf", price=10, old_price="wad",
#                                        sellable_online=True, other_colors="wafg",designer="dwa", depth=42, height=41, width=10,
#                                        category_ID=1)
# furniture = FurnitureTable(
#             item_id=90420332, name='FREKVENS', price=265.0, old_price='No old price',
#             sellable_online=True, other_colors='No', designer='Nicholai Wiig Hansen',
#             depth=54.379202151501566, height=99.0, width=51.0, category_ID=0)
i = 0
Base.metadata.create_all(engine)

with Session(engine) as f_session:
    with f_session.begin():
        for row in df.to_dict(orient='records'):
            i += 1
            furniture = Furniture(id=i, item_id=row["item_id"], name=row["name"], price=row["price"],
                                  old_price=row["old_price"],
                                  sellable_online=row["sellable_online"], other_colors=row["other_colors"],
                                  designer=row["designer"], depth=row["depth"], height=row["height"],
                                  width=row["width"],
                                  category_ID=row["category_ID"])
            f_session.add(furniture)
        f_session.commit()
# stmt = {
#     select(Furniture).join(Furniture.category).where(Category.category == "Bar furniture").where(Furniture.name == "FREKVENS")
#
# }

# with f_session.begin():
#     res = f_session.execute(select(FurnitureTable).join(FurnitureTable.category).where(FurnitureTabl.)
#     # furniture2 = res.scalars()
#     # for item in res.scalars():
#     #     print(item)
#     # print(furniture2.category_ID)

i = 0
with Session(engine) as a_session:
    with a_session.begin():
        for row in df1.to_dict(orient='records'):
            i += 1
            address = Category(id=i, category=row["category"], category_ID=row["category_ID"])
            a_session.add(address)
        a_session.commit()
    with a_session.begin():
        res = a_session.execute(select(Category).where(Category.id == 1))
        address1 = res.scalar()
        print(address1.category)

        stmt = (select(Furniture).join(Furniture.category).where(Category.category == "Beds"))
        res = pd.read_sql(stmt, a_session.bind)
        print(res)



# with Session(engine) as session:
#     stm = select(Furniture).join(Category).where(Category.category_ID == 1)
#     results = session.query(stm).all()
#     print()

    # df_result = pd.DataFrame(result.fetchall(), columns=result.keys())

    # print(df_result)

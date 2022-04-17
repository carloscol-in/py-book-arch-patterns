from typing import Protocol, Set
import allocation.domain.model as model


class AbstractRepository(Protocol):
    def add(self, product: model.Product):
        ...

    def get(self, sku) -> model.Product:
        ...


class TrackingRepository:
    seen: Set[model.Product]

    def __init__(self, repo: AbstractRepository):
        self.seen: Set[model.Product] = set()
        self._repo = repo

    def add(self, product: model.Product):
        self._repo.add(product)
        self.seen.add(product)

    def get(self, sku) -> model.Product:
        product = self._repo.get(sku)
        if product:
            self.seen.add(product)
        return product


class SqlAlchemyRepository:
    def __init__(self, session):
        self.session = session

    def add(self, product):
        self.session.add(product)

    def get(self, sku):
        return self.session \
            .query(model.Product) \
            .filter_by(sku=sku) \
            .first()

from datetime import datetime
from flask import Flask, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import allocation.adapters.orm as orm
from allocation.service_layer import unit_of_work
import allocation.domain.model as model
import allocation.service_layer.handlers as handlers

app = Flask(__name__)
orm.start_mappers()

@app.route('/add_batch', methods=["POST"])
def add_batch():
    """Add batch"""
    eta = request.json['eta']
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    handlers.add_batch(
        request.json['ref'],
        request.json['sku'],
        request.json['qty'],
        eta,
        unit_of_work.SqlAlchemyUnitOfWork()
    )
    return 'OK', 201


@app.route('/allocate', methods=['POST'])
def allocate_endpoint():
    """Allocate"""
    try:
        batchref = handlers.allocate(
            request.json['orderid'], 
            request.json['sku'],
            request.json['qty'],
            unit_of_work.SqlAlchemyUnitOfWork()
        )
    except (model.OutOfStock, handlers.InvalidSku) as e:
        return jsonify({'message': e}), 400

    return jsonify({'batchref': batchref}), 201

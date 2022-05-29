from datetime import datetime
from flask import Flask, jsonify, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from allocation import bootstrap, views

from allocation.service_layer import messagebus, unit_of_work
from allocation.domain import events, commands
import allocation.adapters.orm as orm
import allocation.domain.model as model
import allocation.service_layer.handlers as handlers

app = Flask(__name__)
bus = bootstrap.bootstrap()

@app.route('/add_batch', methods=["POST"])
def add_batch():
    """Add batch"""
    eta = request.json['eta']

    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    
    cmd = commands.CreateBatch(
        request.json['ref'],
        request.json['sku'],
        request.json['qty'],
        eta
    )
    bus.handle(cmd)
    return 'OK', 201


@app.route('/allocate', methods=['POST'])
def allocate_endpoint():
    """Allocate"""
    try:
        event = events.AllocationRequired(
            request.json['orderid'], request.json['sku'], request.json['qty'],
        )
        results = bus.handle(event)
        batchref = results.pop(0)
    except (model.OutOfStock, handlers.InvalidSku) as e:
        return jsonify({'message': e}), 400

    return jsonify({'batchref': batchref}), 201

@app.route('/allocations/<orderid>', methods=['GET'])
def allocations_view_endpoint(orderid):
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    result = views.allocations(orderid, uow)

    if not result:
        return 'not found', 404

    return jsonify(result), 200
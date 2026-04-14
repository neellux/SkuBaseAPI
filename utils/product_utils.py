def get_parent_product_id(product_id: str) -> str:
    if "/" in product_id:
        return "/".join(product_id.split("/")[:-1])
    return product_id

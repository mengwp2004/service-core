package com.rarible.core.entity.reducer.service.service

import com.rarible.core.entity.reducer.service.EntityTemplateProvider
import com.rarible.core.entity.reducer.service.model.Erc20Balance

class Erc20BalanceTemplateProvider : EntityTemplateProvider<Long, Erc20Balance> {
    override fun getEntityTemplate(id: Long): Erc20Balance {
        return Erc20Balance(
            id = id,
            balance = 0,
            revertableEvents = emptyList()
        )
    }
}

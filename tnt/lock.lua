--- Кластерный замок тяжёлых операций.
---
--- Снимок на большой арене съедает диск и процессор, уборка мусора —
--- диск, перебалансировка — сеть. Каждая из них по отдельности переживаема,
--- а все разом на всех узлах превращают штатное обслуживание в аварию.
--- Договориться внутри узла нечем: узлы друг о друге не знают, а оператор,
--- нажавший «снять снимок везде», именно этого и просит.
---
--- Замок живёт в хранилище решений и держится арендой: пока узел
--- продлевает её, ключ существует и занять замок нельзя, а как только
--- продление прекращается, хранилище удаляет ключ само. Узел, упавший
--- с замком в руках, отпускает его без чужого участия — в аварию именно
--- это и важно, потому что снимать застрявший замок руками будет некому.
---
--- Замок рекомендательный, и это надо знать про него заранее. Аренда
--- может истечь, пока операция идёт: связь пропала, продление не дошло,
--- хранилище отдало замок другому — а чекпойнт на этом узле продолжается,
--- потому что прервать его нельзя. Поэтому замок бережёт от одновременного
--- запуска, а не от одновременного выполнения, и не заменяет проверок
--- на самом узле: работа с тем же именем не запускается дважды и без него.
---
--- Отказ хранилища не считается свободным замком. «Занят другим»
--- и «до хранилища не достучаться» выглядят одинаково только
--- для невнимательного кода: во втором случае брать нельзя, потому что
--- держатель, возможно, жив и работает.
---
--- Пользоваться так:
---
---     local lock = require('tnt.lock')
---
---     lock.configure({ client = client, identity = box.info.name })
---
---     local done, err = lock.guarded('snapshot', function()
---         return box.snapshot()
---     end)

local clock = require('tnt.clock')
local fail = require('tnt.must.fail')
local fencing = require('tnt.fencing')
local json = require('json')
local loop = require('tnt.loop')
local external = require('tnt.external')

local log = require('tnt.log').new('tnt.lock')

local Module = {}

--- Где живут замки, если не сказано иное.
local DEFAULT_PREFIX = '/app/locks'

--- На сколько берётся аренда.
---
--- Минута: столько идёт средний чекпойнт, и столько же терпимо ждать
--- освобождения после падения держателя.
local DEFAULT_TTL = 60

--- Через промежуточную ссылку: сроки бывают дробными, а вывод типов
--- по первому присвоению считает их целыми.
---@type any
local settings = {}

---@class TntLockHolding
---@field lease_id string Аренда, которой держится ключ
---@field confirmed_mono number Когда продление подтверждалось в последний раз

--- Замки, которые держит этот узел: имя → аренда.
---@type table<string, TntLockHolding>
local holding = {}

--- Такт продления.
---@type any
local keeper

local source = external.install(Module, {
    monotonic = clock.monotonic,
    realtime = clock.realtime,
})

---@class TntLockSettings
---@field client table Клиент хранилища решений
---@field identity string|nil Имя своего инстанса
---@field prefix string|nil Где живут замки
---@field ttl number|nil На сколько берётся аренда
---@field renew_interval number|nil Как часто продлевается аренда

--- Настраивает замки.
---
--- Негодные настройки бросают на строке вызывающего и ничего не меняют:
--- их пишет программист, чинить надо его строку, а замки живут
--- с прежними настройками, пока он не починит.
---@param opts TntLockSettings
function Module.configure(opts)
    opts = opts or {}

    if opts.client == nil then
        error('замку нужен клиент хранилища решений', 2)
    end

    local ttl = opts.ttl or DEFAULT_TTL

    -- Треть срока: два подряд пропущенных продления ещё не отнимают
    -- замок, три — отнимают, и узнаёт об этом узел раньше хранилища.
    local renew_interval = opts.renew_interval or ttl / 3

    -- Узел считает замок своим срок аренды за вычетом шага продления.
    -- Шаг не меньше срока, как и неположительные числа, такого срока
    -- не дают: узел держал бы замок вечно, а хранилище отдало бы его
    -- другому между продлениями.
    if fencing.renew_deadline(ttl, renew_interval) == nil then
        -- Текст отдельной строкой: уровень броска стоит в одной строке
        -- с вызовом, и проверки видят, если он съедет.
        local refusal =
            'замку нужен положительный срок аренды и шаг продления меньше него, а не срок %s и шаг %s'

        error(refusal:format(tostring(ttl), tostring(renew_interval)), 2)
    end

    settings = {
        client = opts.client,
        identity = opts.identity or 'неизвестный',
        prefix = opts.prefix or DEFAULT_PREFIX,
        ttl = ttl,
        renew_interval = renew_interval,
    }

    holding = {}

    keeper:set_interval(settings.renew_interval)
end

--- Полное имя ключа замка.
---@param name string
---@return string
local function key_of(name)
    return ('%s/%s'):format(settings.prefix, name)
end

--- Сколько можно не подтверждать аренду, оставаясь держателем.
---
--- Не весь срок аренды, а срок за вычетом запаса: подтверждение идёт
--- по сети, и узел, считающий себя держателем до последней секунды,
--- узнаёт о потере замка позже хранилища.
---@return number|nil
local function deadline()
    -- В скобках: причина отказа здесь не нужна, а вторым возвратом она
    -- уехала бы вызывающему, который ждёт одно число.
    return (fencing.renew_deadline(settings.ttl, settings.renew_interval))
end

--- Держит ли узел замок прямо сейчас.
---
--- Ответ опирается на время последнего подтверждения, а не на то, что
--- ключ когда-то был занят: между потерей связи и истечением аренды узел
--- ещё вправе считать замок своим, а после — уже нет.
---@param name string
---@return boolean
function Module.holds(name)
    local held = holding[name]

    if held == nil then
        return false
    end

    local overdue = fencing.should_fence({
        is_leader = true,
        now_mono = source().monotonic(),
        last_confirm_mono = held.confirmed_mono,
        renew_deadline = deadline(),
    })

    return overdue == nil
end

--- Забывает замок, не трогая хранилище.
---
--- Нужен там, где ключа уже нет: аренда истекла, и удалять нечего.
---@param name string
local function forget(name)
    holding[name] = nil
end

--- Отпускает замок.
---
--- Аренда отзывается, а ключ удаляется, только пока срок держания
--- не вышел: после него хранилище вправе было отдать замок другому,
--- и удаление по имени сняло бы чужой замок. Отзыва аренды хватает
--- и тогда — вместе с арендой хранилище снимает ключи, что на ней
--- держатся, а чужой ключ держится на чужой. Пока же замок свой,
--- запросов два: не дойди один, замок освободит другой.
---
--- Отказ хранилища здесь не беда: аренда истечёт сама, и замок
--- освободится — просто позже, чем мог бы.
---@param name string
---@return boolean released
function Module.release(name)
    local held = holding[name]

    if held == nil then
        return false
    end

    local own = Module.holds(name)

    forget(name)

    if own then
        pcall(settings.client.delete, settings.client, key_of(name))
    end

    pcall(settings.client.lease_revoke, settings.client, held.lease_id)

    return true
end

--- Берёт замок.
---
--- Аренда берётся до попытки занять ключ, поэтому проигравший обязан её
--- отозвать: иначе она висит до конца срока и без нужды занимает место
--- в хранилище.
---@param name string
---@return boolean taken
---@return string|nil err
function Module.acquire(name)
    if Module.holds(name) then
        return true
    end

    -- Срок вышел, а аренда всё ещё числится за нами: хранилище к этому
    -- времени уже отдало замок, и держаться за прежнюю аренду значит
    -- не взять его заново никогда. Отпускание за вышедшим сроком ключа
    -- не удаляет: его мог занять другой, и тогда попытка ниже честно
    -- проиграет ему, а не снимет его замок.
    if holding[name] ~= nil then
        Module.release(name)
    end

    local lease, lease_error = settings.client:lease_grant(settings.ttl)

    if lease == nil then
        return false, ('аренда не взята: %s'):format(tostring(lease_error))
    end

    local payload = json.encode({
        identity = settings.identity,
        operation = name,
        since = source().realtime(),
    })

    local taken, taken_error = settings.client:txn_create(key_of(name), payload, lease.id)

    if taken == nil then
        pcall(settings.client.lease_revoke, settings.client, lease.id)

        if taken_error ~= nil and taken_error.category == 'CAS_CONFLICT' then
            return false, ('замок занят: %s'):format(name)
        end

        return false, ('замок не взят: %s'):format(tostring(taken_error))
    end

    holding[name] = { lease_id = lease.id, confirmed_mono = source().monotonic() }

    return true
end

--- Продлевает аренду замка.
---
--- Неудача не отнимает замок сразу: право держать его отмеряется сроком,
--- а не успехом одного запроса. Но и держать дольше срока нельзя —
--- хранилище к тому времени отдаст его другому.
---@param name string
---@return boolean renewed
---@return string|nil err
function Module.renew(name)
    local held = holding[name]

    if held == nil then
        return false, 'замок не взят'
    end

    local renewed, renew_error = settings.client:lease_keepalive(held.lease_id)

    if renewed == nil then
        if renew_error ~= nil and renew_error.category == 'LEASE_EXPIRED' then
            -- Аренда истекла: ключ уже удалён хранилищем, и держаться
            -- за него больше не за что.
            forget(name)

            return false, 'аренда истекла'
        end

        return false, ('аренда не продлена: %s'):format(tostring(renew_error))
    end

    held.confirmed_mono = source().monotonic()

    return true
end

--- Продлевает все удерживаемые замки.
---
--- Зовётся тактом того, кто держит замок долго: операция идёт минутами,
--- а аренда живёт минуту.
---@return string[] lost Замки, которых узел лишился
function Module.renew_all()
    local lost = {}

    for name in pairs(holding) do
        local renewed = Module.renew(name)

        if not renewed and not Module.holds(name) then
            table.insert(lost, name)
        end
    end

    table.sort(lost)

    return lost
end

--- Кто держит замок по мнению хранилища.
---
--- Пустота означает «свободен», отказ — «неизвестно»: путать их нельзя,
--- потому что решения по ним противоположные.
---@param name string
---@return table|nil owner
---@return string|nil err
function Module.owner(name)
    local entry, err = settings.client:get(key_of(name))

    if entry == nil then
        if err == nil then
            return nil
        end

        return nil, tostring(err)
    end

    ---@type boolean, any
    local ok, decoded = pcall(json.decode, entry.value)

    if not ok or type(decoded) ~= 'table' then
        return nil, 'запись замка не разобрана'
    end

    return decoded
end

--- Выполняет тело под замком.
---
--- Замок отпускается, чем бы тело ни кончилось: операция, уронившая узел
--- в исключение, не должна оставлять кластер без обслуживания до конца
--- аренды.
---@param name string
---@param body fun(): any, any
---@return any answer Что вернуло тело; пустота, если замок не взят
---@return any err Почему замок не взят либо второе значение тела
function Module.guarded(name, body)
    local taken, err = Module.acquire(name)

    if not taken then
        -- «Не взят», а не «занят»: причина бывает и отказом хранилища,
        -- и запись не должна выдавать одну за другую.
        log.info('операция пропущена: замок не взят', { operation = name, err = err })

        return nil, err
    end

    local ok, answer, refusal = pcall(body)

    Module.release(name)

    if not ok then
        fail.raise(answer)
    end

    -- Отказ тела — та же пара `nil, err`, что и отказ замка: вызывающий
    -- разбирает оба одинаково, а потерянная причина превратила бы
    -- неудачу операции в молчаливое «ничего не вернула».
    return answer, refusal
end

--- Способ занимать кластер, пригодный для `jobs.configure` работ
--- обслуживания `tnt-maintenance`.
---
--- Тот же договор, что и у местной занятости узла: взяли — вернули
--- способ отпустить, не взяли — вернули причину. Так кластерный замок
--- встаёт рядом с местной защитой, а не вместо неё.
---@param name string Имя операции
---@return (fun())|nil release
---@return string|nil err
function Module.occupy(name)
    local taken, err = Module.acquire(name)

    if not taken then
        return nil, err
    end

    return function()
        Module.release(name)
    end
end

--- Что с замками сейчас.
---
--- Сроки отдаются наружу вместе с держаниями: по одному списку имён
--- не видно, сколько узлу осталось до потери замка.
---@return table
function Module.status()
    local held = {}

    for name in pairs(holding) do
        table.insert(held, name)
    end

    table.sort(held)

    return {
        prefix = settings.prefix,
        identity = settings.identity,
        ttl = settings.ttl,
        renew_interval = settings.renew_interval,
        held = held,
        running = keeper:running(),
    }
end

--- Запускает продление удерживаемых замков.
---
--- Без него замок живёт ровно одну аренду: чекпойнт на большой арене
--- идёт дольше, и хранилище отдаст замок другому прямо посреди работы.
function Module.start()
    keeper:start()
end

--- Останавливает продление.
function Module.stop()
    keeper:stop()
end

--- Идёт ли продление.
---@return boolean
function Module.running()
    return keeper:running()
end

keeper = loop.new({
    name = 'cluster_lock_keeper',

    -- Начальный срок не важен: настоящий ставит `configure`, а до неё
    -- замков нет вовсе и продлевать нечего.
    interval = DEFAULT_TTL,
    tick = function()
        local lost = Module.renew_all()

        for _, name in ipairs(lost) do
            -- Прервать идущую операцию нельзя, и делать вид, что ничего
            -- не случилось, тоже: замок ушёл, и второй узел вправе начать
            -- ту же работу.
            log.warn('замок потерян, а операция идёт', { operation = name })
        end
    end,
    on_error = function(err)
        log.warn('продление замков не отработало', { err = err })
    end,
})

return Module

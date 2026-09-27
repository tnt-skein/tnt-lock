--- Общие средства проверок кластерного замка.
---
--- Исходник замка читается с диска, а не через `require`: у Tarantool
--- свой загрузчик `.rocks`, он идёт раньше `package.path` и подсунул бы
--- установленную копию пакета, если она есть. Проверки тогда шли бы
--- против вчерашнего кода, а покрытие считалось бы по нему. Зависимости
--- пакета — `tnt-clock`, `tnt-external`, `tnt-fencing`, `tnt-log`,
--- `tnt-loop` и `tnt-must` — берутся из `.rocks` обычным `require`:
--- проверяется этот пакет, а не они.
---
--- Двойник хранилища `tnt.etcd.double` нужен только проверкам: клиент
--- etcd замку приходит аргументом. Он приходит из `.rocks` вместе
--- с `tnt-etcd-client` (`make deps`). Двойник берётся у самого клиента:
--- написанный заново, он отличался бы от клиента мелочами, и проверки
--- зависели бы от них.
---
--- Оснастка в `test/testing/` — загрузчик исходников и ловушка журнала —
--- грузится так же, файлами, и один раз на процесс: второй экземпляр
--- загрузчика не знал бы, что вытеснил первый, и не вернул бы вытесненное
--- на место.
---
--- Проверки берут всё через этот помощник, а не из оснастки напрямую:
--- помощник — единственное, чем файл проверок отличается от того же файла
--- в наборе, где пакет живёт рядом со своими зависимостями.

local fio = require('fio')
local t = require('luatest')

--- Модули оснастки в порядке зависимостей: ловушка журнала берёт
--- загрузчик.
local TESTING = {
    { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' },
    { name = 'tnt.testing.journal', path = 'test/testing/journal.lua' },
}

for _, module in ipairs(TESTING) do
    if package.loaded[module.name] == nil then
        local chunk, failure = loadfile(fio.abspath(module.path))

        if chunk == nil then
            error(('оснастка %s не читается: %s'):format(module.name, tostring(failure)))
        end

        package.loaded[module.name] = chunk()
    end
end

local sources = package.loaded['tnt.testing.sources']

local double = require('tnt.etcd.double')

local helper = {}

--- Замок из исходников; соседей в списке нет — их `require` находит
--- в `.rocks`.
helper.MODULES = {
    { name = 'tnt.lock', path = 'tnt/lock.lua' },
}

--- Загружает исходники в `package.loaded` и отдаёт названный модуль.
helper.load = sources.load

--- Убирает исходники и возвращает то, что они вытеснили.
helper.unload = sources.unload

--- Двойник хранилища решений: клиент и его состояние.
---@return table client
---@return table state
function helper.store()
    local client, state = double.new()

    return client, state
end

--- Ловушка журнала на время проверки: «была ли запись с подстрокой»
--- и «забыть накопленное».
helper.capture_log = package.loaded['tnt.testing.journal'].capture

--- Бросок вызова целиком и место, которое он обязан назвать.
---
--- Место — строка тела вызова: тело стоит одной строкой сразу
--- за объявлением функции, и бросок на строке вызывающего называет
--- именно её. Файл берётся тем же именем, каким его называет бросок:
--- путь к файлу проверки бывает и относительным, и урезанным спереди.
---@param call function
---@return string err Бросок целиком
---@return string place `файл:строка` тела вызова
function helper.refusal(call)
    local ok, err = pcall(call)

    t.assert_equals(ok, false)

    local info = debug.getinfo(call, 'S') --[[@as { short_src: string, linedefined: integer }]]

    return tostring(err), ('%s:%d'):format(info.short_src, info.linedefined + 1)
end

return helper
